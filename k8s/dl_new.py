# ==================================================================
"""
Program:
  v1:
    1、更改容器的在NFS端的挂载路径:/mnt/file-storage/platform/username/framework → /mnt/file-storage/platform/username
    2、新增： "nginx.ingress.kubernetes.io/proxy-body-size": "0"
  v2:
    1、深度学习框架采用all-in-one模式
  v3:
    1、添加novnc服务
  v4:
    1、将url中的username字段用hash_code替代
    2、调度时，gpu限额：2GiB
  v5:
    env, vnc url, image tag
History:
    2020/3/2    Harold    V1
    2020/5/15   Harold    v2
    2020/5/21   Harold    v3
    2020/5/29   Harold    v4
    2020/6/14   Harold    v5

"""
# ==================================================================

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
import re
import time
import hashlib
import pymysql
import json

# ==================================================================
# statefulset
# ------------------------------------------------------------------

def create_statefulset(namespace, username, image, resource, hash_code):

    env = [
        client.V1EnvVar(
            name="RELATIVE_URL_ROOT",
            value="{hash_code}_vnc".format(hash_code=hash_code)
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
    # header_string = '{"headers": {"Content-Security-Policy": "frame-ancestors self http://*.*.*.*:3001;"}}'
    header_string = '{"headers": {"Content-Security-Policy": "frame-ancestors self * http://localhost:3001 http://127.0.0.1:3001"}}'
    command = [
        "/bin/sh",
        "-c",
        "source /etc/bash.bashrc; jupyter notebook "
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
        name=username,
        image=image,
        volume_mounts=[volume_mount],
        # when it comes to "ports", use "[]"
        ports=[client.V1ContainerPort(name="jupyter", container_port=8888)],
        # resources=client.V1ResourceRequirements(limits={"aliyun.com/gpu-mem": "3"}),
        command=command,
        env=env
    )

    toleration = client.V1Toleration(
        key="nvidia.com/gpu",
        operator="Exists",
        effect="NoSchedule"
    )

    volume = client.V1Volume(
        name="notebook",
        nfs=client.V1NFSVolumeSource(
            server="172.17.33.155",
            path="/mnt/file-storage/platform/{username}".format(username=username)
        )
    )

    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": username}),
        # when it comes to "containers", use "[]"
        spec=client.V1PodSpec(
            node_selector={"resource": resource},
            volumes=[volume],
            tolerations=[toleration],
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

# ==================================================================
# service
# ------------------------------------------------------------------

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

# ==================================================================
# ingress
# ------------------------------------------------------------------

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
                host="bdcdev.cuc.edu.cn",
                http=client.ExtensionsV1beta1HTTPIngressRuleValue(
                    paths=[
                        client.ExtensionsV1beta1HTTPIngressPath(
                            path="/{hash_code}_jupyter".format(hash_code=hash_code),
                            backend=client.ExtensionsV1beta1IngressBackend(
                                service_port=8888,
                                service_name=namespace + '-' + username)
                        ),
                        client.ExtensionsV1beta1HTTPIngressPath(
                            path="/{hash_code}_vnc".format(hash_code=hash_code),
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

# ==================================================================
# pod_status
# ------------------------------------------------------------------

def pod_status(namespace, username):

    core_v1_api = client.CoreV1Api()

    count = 30
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
        print('Not enough GPU!')
        return False

# ==================================================================
# start_vnc
# ------------------------------------------------------------------

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
        "mv /root/README.txt /root/Desktop/;"
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

# ==================================================================
# jupyter_token
# ------------------------------------------------------------------

def get_jupyter_token(namespace, username):

    # Calling exec and waiting for response
    exec_command = [
        '/bin/sh',
        '-c',
        'jupyter notebook list']

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
# mysql
# ------------------------------------------------------------------

class DB:
    def __init__(self, host, port, user, password, db, charset):
        self.connect = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset=charset)
        self.cursor = self.connect.cursor()

    def insert_data(self, table_name, userID, hash_code):
        sql = """insert into {table_name}(
        userID, hash_code)
        values ('{userID}', '{hash_code}')""".format(table_name=table_name, userID=userID, hash_code=hash_code)

        try:
            self.cursor.execute(sql)
            self.connect.commit()
        except:
            self.connect.rollback()

    def query_data(self, table_name, userID):
        sql = "select * from {table_name} where userID='{userID}';".format(table_name=table_name, userID=userID)

        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            print(results)
        except:
            print("Error: unable to fetch data")

    def query_privilege(self, table_name, userID):
        sql = "select if (exists(select * from {table_name} where userID='{userID}' limit 1), 1, 0);".format(table_name=table_name, userID=userID)

        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
        except:
            print("Error: unable to fetch data")
        if results[0][0] == 1:
            return True
        else:
            return False

    def __del__(self):
        self.cursor.close()
        self.connect.close()

# ==================================================================
# dl_create
# jupyter url: http://bdcdev.cuc.edu.cn/{hash_code}_jupyter/?token={jupyter_token}
# vnc url: http://bdcdev.cuc.edu.cn/{hash_code}_vnc/static/vnc.html?resize=scale&autoconnect=true&path={hash_code}_vnc/websockify
# vnc url: http://bdcdev.cuc.edu.cn/{hash_code}_vnc/
# ------------------------------------------------------------------

def dl_create(username, namespace="research", resource="gpu", image="172.17.33.146:1180/dl-framework/harold/deepo_vnc_vscode:all-jupyter-py36-cu101-v2"):

    config.load_kube_config(config_file="config_s")

    hash_code = hashlib.md5(username.encode("utf-8")).hexdigest()
    db = DB(host='172.17.33.71', port=30802, user='root', password='123456', db='science_platform', charset='utf8')
    db.insert_data('user_hash', username, hash_code)
    db.query_data('user_hash', username)

    privilege = db.query_privilege('privileged_user', username)
    if privilege:
        resource = "privilege"
        print(resource)

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
    # jsonify = {"jupyter_token": jupyter_token}
    # print(repr(jsonify))
    # json_jupyter_token = json.dumps(jsonify)
    # print((json_jupyter_token))
    return jupyter_token, hash_code

if __name__ == '__main__':

   mood = dl_create('harold')
   print("jupyter_token: %s \nhash_code: %s" % (mood[0], mood[1]))
