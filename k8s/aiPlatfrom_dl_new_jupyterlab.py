from kubernetes import client, config
from kubernetes.stream import stream
import re
import time
import hashlib


def create_statefulset(namespace, username, image, resource, hash_code):

    env = [
        client.V1EnvVar(
            name="RELATIVE_URL_ROOT",
            value="ai/{hash_code}_vnc".format(hash_code=hash_code)
        ),
        # client.V1EnvVar(
        #     name="RESOLUTION",
        #     # value="2265x1080"
        #     # value="2265x1050"
        #     # value="2048x1080"
        #     value="1920x1080"
        # )
    ]

    # command
    header_string = '{"headers": {"Content-Security-Policy": "frame-ancestors self * http://aicloud.cuc.edu.cn"}}'

    command = [
        "/bin/sh",
        "-c",
        "source /etc/bash.bashrc; jupyter lab "
        "--NotebookApp.tornado_settings='{headers}' "
        "--NotebookApp.base_project_url='/ai/{hash_code}_jupyter' "
        "--notebook-dir=/workspace "
        "--ip 0.0.0.0 --no-browser --allow-root".format(headers=header_string, hash_code=hash_code)
    ]

    volume_mount = client.V1VolumeMount(
        name="notebook",
        mount_path="/workspace"
    )

    container = client.V1Container(
        name=username,
        image=image,
        volume_mounts=[volume_mount],
        # when it comes to "ports", use "[]"
        ports=[client.V1ContainerPort(name="jupyter", container_port=8888)],
        # 申请资源：2核CPU(2000毫核心），4GB内存
        resources=client.V1ResourceRequirements(requests={"cpu": "2000m", "memory": "4000Mi"}),
        # resources=client.V1ResourceRequirements(limits={"aliyun.com/gpu-mem": "3"}),
        command=command,
        env=env
    )

    volume = client.V1Volume(
        name="notebook",
        nfs=client.V1NFSVolumeSource(
            server="172.17.33.178",
            path="/fs/vg151748-Research/{username}".format(username=username)
        )
    )

    if resource == "gpu":
        toleration = client.V1Toleration(
            key="nvidia.com/gpu",
            operator="Exists",
            effect="NoSchedule"
        )

        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": username}),
            # when it comes to "containers", use "[]"
            spec=client.V1PodSpec(
                # node_name="gpu09-tesla-p100",
                node_selector={"resource": resource},
                volumes=[volume],
                tolerations=[toleration],
                containers=[container]
            )
        )
    else:
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": username}),
            # when it comes to "containers", use "[]"
            spec=client.V1PodSpec(
                # node_name="bdcenter-k8s-worker9",
                volumes=[volume],
                containers=[container]
            )
        )

    spec = client.V1StatefulSetSpec(
        service_name=username,
        replicas=1,
        selector=client.V1LabelSelector(match_labels={"app": username}),
        template=template
    )

    body = client.V1StatefulSet(
        api_version="apps/v1",
        kind="StatefulSet",
        metadata=client.V1ObjectMeta(name=username),
        spec=spec
    )

    try:
        apps_v1_api = client.AppsV1Api()
        apps_v1_api.create_namespaced_stateful_set(namespace=namespace, body=body)
        return True
    except:
        print("Unexpected sts error")
        return False


def create_service(namespace, username):

    body = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(name=namespace + '-' + username),
        spec=client.V1ServiceSpec(
            selector={"app": username},
            type="NodePort",
            ports=[
                client.V1ServicePort(
                    name="jupyter",
                    port=8888,
                    target_port=8888),
                client.V1ServicePort(
                    name="vnc",
                    port=80,
                    target_port=80)
            ]
        )
    )

    try:
        core_v1_api = client.CoreV1Api()
        core_v1_api.create_namespaced_service(namespace=namespace, body=body)
        return True
    except:
        print("Unexpected svc error")
        return False


def creat_ingress(namespace, username, hash_code):
    body = client.ExtensionsV1beta1Ingress(
        api_version="extensions/v1beta1",
        kind="Ingress",
        metadata=client.V1ObjectMeta(name=username, annotations={
            "kubernetes.io/ingress.class": "nginx",
            "nginx.ingress.kubernetes.io/proxy-body-size": "0",
            "nginx.ingress.kubernetes.io/proxy-send-timeout": "3600",
            "nginx.ingress.kubernetes.io/proxy-read-timeout": "3600"
            # "nginx.ingress.kubernetes.io/rewrite-target": "/{username}_jupyter".format(username=username)
        }),
        spec=client.ExtensionsV1beta1IngressSpec(
            rules=[client.ExtensionsV1beta1IngressRule(
                host="aicloud.cuc.edu.cn",
                http=client.ExtensionsV1beta1HTTPIngressRuleValue(
                    paths=[
                        client.ExtensionsV1beta1HTTPIngressPath(
                            path="/ai/{hash_code}_jupyter".format(hash_code=hash_code),
                            backend=client.ExtensionsV1beta1IngressBackend(
                                service_port=8888,
                                service_name=namespace + '-' + username)
                        ),
                        client.ExtensionsV1beta1HTTPIngressPath(
                            path="/ai/{hash_code}_vnc".format(hash_code=hash_code),
                            backend=client.ExtensionsV1beta1IngressBackend(
                                service_port=80,
                                service_name=namespace + '-' + username)
                        )
                    ]
                )
            )]
        )
    )

    try:
        extensions_v1_beta1_api = client.ExtensionsV1beta1Api()
        extensions_v1_beta1_api.create_namespaced_ingress(namespace=namespace, body=body)
        return True
    except:
        print("Unexpected ing error")
        return False


def pod_status(namespace, username):

    core_v1_api = client.CoreV1Api()

    count = 100
    while count > 0:
        try:
            resp = core_v1_api.read_namespaced_pod(name=username + "-0", namespace=namespace)
            print(resp.status.phase)
            if resp.status.phase == "Running":
                return True
        except:
            print("Unexpected pod error")
            return False
        count -= 1
        time.sleep(2)
    if resp.status.phase != "Running":
        print('Failed to create jupyter!')
        return False


def start_vnc(namespace, username):

    exec_command = [
        "/bin/sh",
        "-c",
        "setsid /startup.sh > /dev/null 2>&1 &"
    ]
    try:
        apps_v1_api = client.CoreV1Api()
        print("starting vnc")
        stream(apps_v1_api.connect_get_namespaced_pod_exec,
               name=username + "-0",
               namespace=namespace,
               command=exec_command,
               stderr=True, stdin=False,
               stdout=True, tty=False)
        print("vnc started")
        return True
    except:
        print("Failed to start vnc")
        return False

# ==================================================================
# create app's shortcuts on vnc desktop
# ------------------------------------------------------------------

def create_shorcut(namespace, username):

    exec_command = [
        "/bin/sh",
        "-c",
        # "mv /root/README.txt /root/Desktop/;"
        "cd /usr/share/applications/;"
        "cp code.desktop firefox.desktop lxterminal.desktop pcmanfm.desktop vim.desktop leafpad.desktop gpicview.desktop /root/Desktop/"
    ]
    try:
        apps_v1_api = client.CoreV1Api()
        print("creating shortcuts")
        stream(apps_v1_api.connect_get_namespaced_pod_exec,
               name=username + "-0",
               namespace=namespace,
               command=exec_command,
               stderr=True, stdin=False,
               stdout=True, tty=False)
        print("shortcuts created")
        return True
    except:
        print("Failed to create shortcut")
        return False


def get_jupyter_token(namespace, username):

    # Calling exec and waiting for response
    exec_command = [
        '/bin/sh',
        '-c',
        'jupyter notebook list']
        # 'jupyter lab list']


    try:
        apps_v1_api = client.CoreV1Api()
        resp = stream(apps_v1_api.connect_get_namespaced_pod_exec,
                      name=username + "-0",
                      namespace=namespace,
                      command=exec_command,
                      stderr=True, stdin=False,
                      stdout=True, tty=False)
        jupyter_token = str(re.findall(r"token=(.+) :", resp)[0])
        return jupyter_token
    except:
        print("Failed to get jupyter_token")
        jupyter_token = ""
        return jupyter_token


# ==================================================================
# dl_create
# jupyter url: http://aicloud.cuc.edu.cn/ai/{hash_code}_jupyter/?token={jupyter_token}
# vnc url: http://aicloud.cuc.edu.cn/ai/{hash_code}_vnc/
# ------------------------------------------------------------------

def dl_create(username, namespace, resource, image):

    config.load_kube_config(config_file="config")
    hash_code = hashlib.md5(username.encode("utf-8")).hexdigest()

    sts_created = create_statefulset(namespace, username, image, resource, hash_code)
    svc_created = create_service(namespace, username)
    ing_created = creat_ingress(namespace, username, hash_code)
    po_created = pod_status(namespace, username)

    if sts_created and svc_created and ing_created and po_created:
        start_vnc(namespace, username)
        jupyter_token = get_jupyter_token(namespace, username)
        time.sleep(2)
        create_shorcut(namespace, username)
    else:
        jupyter_token = ""
    return jupyter_token, hash_code

if __name__ == '__main__':
   mood = dl_create(username='jack', namespace='aicloud', resource='cpu', image="172.17.33.146:1180/dl-framework/haroldmua/all-py36-cpu-vnc:v1")
   # mood = dl_create(username='c', namespace='aicloud', resource='gpu', image="172.17.33.146:1180/dl-framework/haroldmua/all-py36-cu102-vnc:v1")
   print("jupyter_token: %s \nhash_code: %s" % (mood[0], mood[1]))
