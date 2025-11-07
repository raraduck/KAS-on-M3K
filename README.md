# KAS-on-M3K

## 1. Install M3K cluster
Setting up with Port exposure and Host volume 
```bash
minikube start \
    --profile=<MINIKUBE_PROFILE_NAME>
    --driver=docker \
    --cpus=24 \
    --memory=200g \
    --container-runtime=docker \
    --gpus=all \
    --mount \
    --mount-string="<HOSTPATH_KAFKA>:/opt/kafka" \
    --mount \
    --mount-string="<HOSTPATH_AIRFLOW>:/opt/airflow" \
    --mount \
    --mount-string="<HOSTPATH_SPARK>:/opt/spark" \
```

### 1.1. Install kubectl
ref: https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/ 

in ~/.bashrc
```bash
alias kubectl="minikube kubectl --"
```
### 1.2. Kubectl Context and Minikube Profile with Config (KUBECONFIG) and Env(MINIKUBE_HOME)
정석적인 방법은 context와 profile 을 이용하는 방법!

```bash
# kubectl:
kubectl config get-contexts
kubectl config use-context <context_name>
# Minikube:
minikube -p <profile_name> status
minikube start -p <profile_name>
minikube delete -p <profile_name>
```
### 1.3. Check GPU Access from Pod nvidia-smi
> nvidia-smi-check.yaml
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: nvidia-smi
  namespace: default
spec:
  restartPolicy: Never
  containers:
    - name: nvidia-smi
      image: nvidia/cuda:12.2.0-base-ubuntu22.04   # ✅ CUDA 베이스 이미지
      command: ["nvidia-smi"]
      resources:
        limits:
          nvidia.com/gpu: 1                        # ✅ GPU 요청 (필수)
```
```bash
kubectl create -f nvidia-smi-check.yaml
```

## 2. Install Kafka
```bash
helm repo add \
    bitnami https://charts.bitnami.com/bitnami
helm repo update
```
```bash
helm install kafka bitnami/kafka \
  --namespace kafka \
  --create-namespace \
  -f values.yaml
```

## 3. Producer and Consumer for Kafka Test
### 3.0. Customize App Images
**Producer**
```bash
# docker build -f Dockerfile.producer -t dwnusa/smd-producer:v0.0.1-amd64 .

kubectl run kafka-producer --restart='Never'   --image dwnusa/smd-producer:v0.0.1-amd64
```
**Consumer**
```bash
# docker build -f Dockerfile.consumer -t dwnusa/smd-consumer:v0.0.1-amd64 .

kubectl run kafka-consumer --restart='Never'   --image dwnusa/smd-consumer:v0.0.1-amd64
```
### 3.1. Create Topic 
```bash
# Topic 생성
```bash
kubectl exec -it kafka-controller-0 -n kafka \
  -- kafka-topics.sh --create \
  --topic server-machine-usage \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 \
  --replication-factor 1 \
  --partitions 3

kafka-topics.sh --list --bootstrap-server kafka.kafka.svc.cluster.local:9092
```

