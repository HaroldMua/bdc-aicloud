from kubernetes import client, config
from kubernetes.stream import stream
import re
import time
import hashlib


def create_statefulset(namespace, username, image, hash_code):
    # command
    header_string = '{"headers": {"Content-Security-Policy": "frame-ancestors self * http://ep.bdc.cuc.edu.cn"}}'
    command = [
        "/bin/sh",
        "-c",
        "source /etc/bash.bashrc; jupyter lab "
        "--NotebookApp.tornado_settings='{headers}' "
        "--NotebookApp.base_project_url='/{hash_code}_jupyter' "
        "--notebook-dir=/workspace "
        "--ip 0.0.0.0 --no-browser --allow-root".format(headers=header_string, hash_code=hash_code)
    ]

    volume_mount = client.V1VolumeMount(
        name="notebook",
        mount_path="/workspace"
    )

    container = client.V1Container(
        name=username.lower(),
        image=image,
        volume_mounts=[volume_mount],
        # when it comes to "ports", use "[]"
        ports=[client.V1ContainerPort(name="jupyter", container_port=8888)],
        command=command,
    )

    volume = client.V1Volume(
        name="notebook",
        nfs=client.V1NFSVolumeSource(
            server="172.17.33.178",
            path="/fs/vg151748-School/experiment_nfs_data/DL/{username}".format(username=username)
        )
    )

    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": username.lower()}),
        # when it comes to "containers", use "[]"
        spec=client.V1PodSpec(
            volumes=[volume],
            containers=[container]
        )
    )

    spec = client.V1StatefulSetSpec(
        service_name=username.lower(),
        replicas=1,
        selector=client.V1LabelSelector(match_labels={"app": username.lower()}),
        template=template
    )

    body = client.V1StatefulSet(
        api_version="apps/v1",
        kind="StatefulSet",
        metadata=client.V1ObjectMeta(name=username.lower()),
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
        metadata=client.V1ObjectMeta(name=namespace + '-' + username.lower()),
        spec=client.V1ServiceSpec(
            selector={"app": username.lower()},
            type="NodePort",
            ports=[
                client.V1ServicePort(
                    name="jupyter",
                    port=8888,
                    target_port=8888)
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
        metadata=client.V1ObjectMeta(name=username.lower(), annotations={
            "kubernetes.io/ingress.class": "nginx",
            "nginx.ingress.kubernetes.io/proxy-body-size": "0",
            "nginx.ingress.kubernetes.io/proxy-send-timeout": "3600",
            "nginx.ingress.kubernetes.io/proxy-read-timeout": "3600"
            # "nginx.ingress.kubernetes.io/rewrite-target": "/{username}_jupyter".format(username=username)
        }),
        spec=client.ExtensionsV1beta1IngressSpec(
            rules=[client.ExtensionsV1beta1IngressRule(
                host="ep.bdc.cuc.edu.cn",
                http=client.ExtensionsV1beta1HTTPIngressRuleValue(
                    paths=[
                        client.ExtensionsV1beta1HTTPIngressPath(
                            path="/{hash_code}_jupyter".format(hash_code=hash_code),
                            backend=client.ExtensionsV1beta1IngressBackend(
                                service_port=8888,
                                service_name=namespace + '-' + username.lower())
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

    count = 30
    while count > 0:
        try:
            resp = core_v1_api.read_namespaced_pod(name=username.lower() + "-0", namespace=namespace)
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

def get_jupyter_token(namespace, username):
    # Calling exec and waiting for response
    exec_command = [
        '/bin/sh',
        '-c',
        'jupyter notebook list']

    try:
        apps_v1_api = client.CoreV1Api()
        resp = stream(apps_v1_api.connect_get_namespaced_pod_exec,
                      name=username.lower() + "-0",
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
# jupyter url: http://ep.bdc.cuc.edu.cn/{hash_code}_jupyter/?token={jupyter_token}
# ------------------------------------------------------------------

def dl_create(username, namespace="experiment-dl", image="172.17.33.146:1180/dl-framework/haroldmua/py36:v1"):
    config.load_kube_config(config_file="config")
    hash_code = hashlib.md5(username.encode("utf-8")).hexdigest()

    sts_created = create_statefulset(namespace, username, image, hash_code)
    svc_created = create_service(namespace, username)
    ing_created = creat_ingress(namespace, username, hash_code)
    po_created = pod_status(namespace, username)

    if sts_created and svc_created and ing_created and po_created:
        jupyter_token = get_jupyter_token(namespace, username)
        time.sleep(2)
    else:
        jupyter_token = ""
    return jupyter_token, hash_code

if __name__ == '__main__':
   mood = dl_create('TEST')
   print("jupyter_token: %s \nhash_code: %s" % (mood[0], mood[1]))
