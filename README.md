learning CRD

---

from this awesome [ book ]( https://book.kubebuilder.io/cronjob-tutorial )

modified point:
1. my k8s is arm64, so all the images have been used will be all arm64
	1. kube-rbac-proxy
	1. base image distroless
	1. myself controller
1. since I don't have docker, so I use podman instead
1. since I don't install whole kubebuilder into /usr/bin/kubebuilder
	1. so the test will failed due to it cannot find etcd under that folder
	1. so I skip the test step while build the image
