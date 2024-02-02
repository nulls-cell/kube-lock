package pkg

import (
	"bytes"
	"context"
	"fmt"
	stackerror "github.com/nulls-cell/stackerror/pkg/error"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	rl "k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/utils/clock"
)

const defaultTimeoutDur = time.Second * 30

type LockConfig struct {
	// Lock is the resource that will be used for locking
	Lock          rl.Interface
	LockNamespace string
	LockName      string
	Timeout       time.Duration
}

// KubeLock is a leader election client.
type KubeLock struct {
	Config LockConfig
	// internal bookkeeping
	observedRecord    rl.LeaderElectionRecord
	observedRawRecord []byte
	observedTime      time.Time
	// clock is wrapper around time to allow for less flaky testing
	clock clock.Clock

	// used to lock the observedRecord
	observedRecordLock sync.Mutex
	stopCh             chan struct{}
}

// IsLeader returns true if the last observed leader was this client else returns false.
func (pcls *KubeLock) isLeader() bool {
	return pcls.getObservedRecord().HolderIdentity == pcls.Config.Lock.Identity()
}

func (pcls *KubeLock) GetIdentity() string {
	return pcls.Config.Lock.Identity()
}

// Acquire loops calling tryAcquireOrRenew and returns true immediately when tryAcquireOrRenew succeeds.
// Returns false if ctx signals done.
func (pcls *KubeLock) Acquire(ctx context.Context) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	succeeded := false
	desc := pcls.Config.Lock.Describe()
	klog.Infof("attempting to acquire leader lease, identity[%v], %v...", pcls.GetIdentity(), desc)
	succeeded = pcls.tryAcquireOrRenew(ctx)
	if !succeeded {
		klog.Infof("failed to acquire lease %v", desc)
		return false
	}
	pcls.Config.Lock.RecordEvent("success acquire lock")
	klog.Infof("successfully acquired lease %v", desc)
	return succeeded
}

// Release attempts to release the leader lease if we have acquired it.
func (pcls *KubeLock) Release() bool {
	if !pcls.isLeader() {
		return true
	}
	now := metav1.NewTime(pcls.clock.Now())
	leaderElectionRecord := rl.LeaderElectionRecord{
		LeaderTransitions:    pcls.observedRecord.LeaderTransitions,
		LeaseDurationSeconds: 10,
		RenewTime:            now,
		AcquireTime:          now,
	}
	if err := pcls.Config.Lock.Update(context.TODO(), leaderElectionRecord); err != nil {
		klog.Errorf("Failed to release lock: %v", err)
		return false
	}

	pcls.setObservedRecord(&leaderElectionRecord)
	return true
}

// tryAcquireOrRenew tries to acquire a leader lease if it is not already acquired,
// else it tries to renew the lease if it has already been acquired. Returns true
// on success else returns false.
func (pcls *KubeLock) tryAcquireOrRenew(ctx context.Context) bool {
	now := metav1.NewTime(pcls.clock.Now())
	leaderElectionRecord := rl.LeaderElectionRecord{
		HolderIdentity:       pcls.Config.Lock.Identity(),
		LeaseDurationSeconds: int(pcls.Config.Timeout / time.Second),
		RenewTime:            now,
		AcquireTime:          now,
	}

	oldLeaderElectionRecord, oldLeaderElectionRawRecord, err := pcls.Config.Lock.Get(ctx)
	if err != nil {
		if !errors.IsNotFound(err) {
			klog.Errorf("error retrieving resource lock %v: %v", pcls.Config.Lock.Describe(), err)
			return false
		}
		if err = pcls.Config.Lock.Create(ctx, leaderElectionRecord); err != nil {
			klog.Errorf("error initially creating leader election record: %v", err)
			return false
		}
		return true
	}

	// 3. Record obtained, check the Identity & Time
	if !bytes.Equal(pcls.observedRawRecord, oldLeaderElectionRawRecord) {
		pcls.setObservedRecord(oldLeaderElectionRecord)
		pcls.observedRawRecord = oldLeaderElectionRawRecord
	}
	if len(oldLeaderElectionRecord.HolderIdentity) > 0 && pcls.isLeaseValid(now.Time) && !pcls.isLeader() {
		klog.V(4).Infof("lock is held by %v and has not yet expired", oldLeaderElectionRecord.HolderIdentity)
		return false
	}

	// 4. We're going to try to update. The leaderElectionRecord is set to it's default
	// here. Let's correct it before updating.
	if pcls.isLeader() {
		leaderElectionRecord.AcquireTime = oldLeaderElectionRecord.AcquireTime
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions
	} else {
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions + 1
	}

	// update the lock itself
	if err = pcls.Config.Lock.Update(ctx, leaderElectionRecord); err != nil {
		klog.Errorf("Failed to update lock: %v", err)
		return false
	}

	pcls.setObservedRecord(&leaderElectionRecord)
	return true
}

func (pcls *KubeLock) isLeaseValid(now time.Time) bool {
	record := pcls.getObservedRecord()
	return record.RenewTime.Add(time.Duration(record.LeaseDurationSeconds+2) * time.Second).After(now)
	//return pcls.observedTime.Add(time.Second * time.Duration(pcls.getObservedRecord().LeaseDurationSeconds)).After(now)
}

// setObservedRecord will set a new observedRecord and update observedTime to the current time.
// Protect critical sections with lock.
func (pcls *KubeLock) setObservedRecord(observedRecord *rl.LeaderElectionRecord) {
	pcls.observedRecordLock.Lock()
	defer pcls.observedRecordLock.Unlock()
	pcls.observedRecord = *observedRecord
	pcls.observedTime = pcls.clock.Now()
}

// getObservedRecord returns observersRecord.
// Protect critical sections with lock.
func (pcls *KubeLock) getObservedRecord() rl.LeaderElectionRecord {
	pcls.observedRecordLock.Lock()
	defer pcls.observedRecordLock.Unlock()
	return pcls.observedRecord
}

// SetTimeout set timeout duration
func (pcls *KubeLock) SetTimeout(timeout time.Duration) {
	pcls.Config.Timeout = timeout
}

func NewHostLock(namespace, name string, kubeClient *kubernetes.Clientset) (*KubeLock, stackerror.IStackError) {
	identityPrefix, err := GetHostIdentity()
	if err != nil {
		return nil, err
	}
	identity := fmt.Sprintf("%v-%v", identityPrefix, name)
	kl, err := newLock(namespace, name, identity, kubeClient)
	return kl, err
}

func NewProcessLock(namespace, name string, kubeClient *kubernetes.Clientset) (*KubeLock, stackerror.IStackError) {
	identityPrefix := GetProcessIdentity()
	identity := fmt.Sprintf("%v-%v", identityPrefix, name)
	return newLock(namespace, name, identity, kubeClient)
}

func NewInstanceLock(namespace, name string, kubeClient *kubernetes.Clientset) (*KubeLock, stackerror.IStackError) {
	identityPrefix := GetInstanceIdentity()
	identity := fmt.Sprintf("%v-%v", identityPrefix, name)
	return newLock(namespace, name, identity, kubeClient)
}

func newLock(namespace, name, identity string, kubeClient *kubernetes.Clientset) (*KubeLock, stackerror.IStackError) {
	rcl := rl.ResourceLockConfig{
		Identity: identity,
	}
	leaseLock, err := rl.New(rl.LeasesResourceLock, namespace, name, kubeClient.CoreV1(), kubeClient.CoordinationV1(), rcl)
	if err != nil {
		return nil, stackerror.WrapStackError(err)
	}
	lc := LockConfig{
		Lock:          leaseLock,
		Timeout:       defaultTimeoutDur,
		LockNamespace: namespace,
		LockName:      name,
	}
	kl := &KubeLock{
		Config:             lc,
		observedRecordLock: sync.Mutex{},
		clock:              clock.RealClock{},
		stopCh:             make(chan struct{}),
	}
	return kl, nil
}
