package pkg

import (
	"context"
	"fmt"
	stackerror "github.com/nulls-cell/stackerror/pkg/error"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func getKubeConfig() (*rest.Config, stackerror.IStackError) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return nil, stackerror.WrapStackError(err)
	}
	kubeConfPath := strings.Join([]string{homedir, ".kube/config"}, string(os.PathSeparator))
	if os.Getenv("KUBECONFIG") != "" {
		kubeConfPath = os.Getenv("KUBECONFIG")
		klog.Infof("found KUBECONFIG env variable has set to %v, will use it to instantiate kube config", kubeConfPath)
	}
	_, err = os.Stat(kubeConfPath)
	if err != nil {
		msg := fmt.Sprintf("not found the kube config path[%v], err[%v]", kubeConfPath, err.Error())
		return nil, stackerror.NewStackError(msg)
	}
	kubeConf, err := clientcmd.BuildConfigFromFlags("", kubeConfPath)
	if err != nil {
		return nil, stackerror.WrapStackError(err)
	}
	return kubeConf, nil
}

func getKubeClient() (*kubernetes.Clientset, stackerror.IStackError) {
	kubeConf, err := getKubeConfig()
	if err != nil {
		return nil, err
	}
	kubeClient, _err := kubernetes.NewForConfig(kubeConf)
	if _err != nil {
		return nil, stackerror.WrapStackError(_err)
	}
	return kubeClient, nil
}

func TestHostLock(t *testing.T) {
	namespace := "default"
	name := "test-host"
	kubeClient, err := getKubeClient()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	ctx := context.TODO()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		lock, _err := NewHostLock(namespace, name, kubeClient)
		if _err != nil {
			fmt.Println(_err.Error())
			return
		}
		ran := rand.Intn(5)
		fmt.Printf("func1 %v\n", ran)
		time.Sleep(time.Duration(ran) * time.Second)
		lock.Acquire(ctx)
	}()
	go func() {
		defer wg.Done()
		lock, _err := NewHostLock(namespace, name, kubeClient)
		if _err != nil {
			fmt.Println(_err.Error())
			return
		}
		ran := rand.Intn(5)
		fmt.Printf("func2 %v\n", ran)
		time.Sleep(time.Duration(ran) * time.Second)
		lock.Acquire(ctx)
	}()
	wg.Wait()
	time.Sleep(2 * time.Second)
}

func TestProcessLock(t *testing.T) {
	namespace := "default"
	name := "test-process"
	kubeClient, err := getKubeClient()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	ctx := context.TODO()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		lock, _err := NewProcessLock(namespace, name, kubeClient)
		if _err != nil {
			fmt.Println(_err.Error())
			return
		}
		ran := rand.Intn(5)
		fmt.Printf("func1 %v\n", ran)
		time.Sleep(time.Duration(ran) * time.Second)
		lock.Acquire(ctx)
	}()
	go func() {
		defer wg.Done()
		lock, _err := NewProcessLock(namespace, name, kubeClient)
		if _err != nil {
			fmt.Println(_err.Error())
			return
		}
		ran := rand.Intn(5)
		fmt.Printf("func2 %v\n", ran)
		time.Sleep(time.Duration(ran) * time.Second)
		lock.Acquire(ctx)
	}()
	wg.Wait()
	time.Sleep(2 * time.Second)
}

func TestInstanceLock(t *testing.T) {
	namespace := "default"
	name := "test-instance"
	kubeClient, err := getKubeClient()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	ctx := context.TODO()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		lock, _err := NewInstanceLock(namespace, name, kubeClient)
		if _err != nil {
			fmt.Println(_err.Error())
			return
		}
		ran := rand.Intn(5)
		fmt.Printf("func1 %v\n", ran)
		time.Sleep(time.Duration(ran) * time.Second)
		lock.Acquire(ctx)
	}()
	go func() {
		defer wg.Done()
		lock, _err := NewInstanceLock(namespace, name, kubeClient)
		if _err != nil {
			fmt.Println(_err.Error())
			return
		}
		ran := rand.Intn(5)
		fmt.Printf("func2 %v\n", ran)
		time.Sleep(time.Duration(ran) * time.Second)
		lock.Acquire(ctx)
	}()
	wg.Wait()
	time.Sleep(2 * time.Second)
}
