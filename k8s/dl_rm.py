from kubernetes import client, config
from kubernetes.client.rest import ApiException
import time
import pymysql

def delete_statefulset(namespace, username):

    try:
        apps_v1_api = client.AppsV1Api()
        apps_v1_api.delete_namespaced_stateful_set(name=username, namespace=namespace)
        print("sts deleted!")
    except:
        print("No sts!")

def delete_service(namespace, username):

    try:
        core_v1_api = client.CoreV1Api()
        core_v1_api.delete_namespaced_service(name=namespace + '-' + username, namespace=namespace)
        print("svc deleted!")
    except:
        print("No svc!")

def delete_ingress(namespace, username):

    try:
        extensions_v1_beta1_api = client.ExtensionsV1beta1Api()
        extensions_v1_beta1_api.delete_namespaced_ingress(namespace=namespace, name=username)
        print("ing deleted!")
    except:
        print("No ing!")

def pod_status(namespace, username):

    count = 40
    while count > 0:
        try:
            core_v1_api = client.CoreV1Api()
            resp = core_v1_api.read_namespaced_pod(name=username + "-0", namespace=namespace)
            print("Pod {name} still ".format(name=username) + str.lower(resp.status.phase))
        except ApiException as e:
            if e.status == 404:
                print("All deleted!")
                return 1
        except:
            print("Unexpected pod error")
            return 0
        count -= 1
        time.sleep(3)

class DB:
    def __init__(self, host, port, user, password, db, charset):
        self.connect = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset=charset)
        self.cursor = self.connect.cursor()
    def query_data(self, table_name, userID):
        sql = "select * from {table_name} where userID='{userID}';".format(table_name=table_name, userID=userID)

        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            print(results)
        except:
            print("Error: unable to fetch data")

    def delete_data(self, table_name, userID):
        sql = "delete from {table_name} where userID='{userID}'".format(table_name=table_name, userID=userID)

        try:
            self.cursor.execute(sql)
            self.connect.commit()
        except:
            self.connect.rollback()

    def __del__(self):
        self.cursor.close()
        self.connect.close()


def delete_dl(username, namespace="research"):

    config.load_kube_config(config_file="config_s")

    db = DB(host='172.17.33.71', port=30802, user='root', password='123456', db='science_platform', charset='utf8')
    db.delete_data('user_hash', username)
    db.query_data('user_hash', username)

    delete_statefulset(namespace, username)
    delete_service(namespace, username)
    delete_ingress(namespace, username)
    status = pod_status(namespace, username)
    return status

if __name__ == '__main__':

    mood = delete_dl("harold")
    print(mood)



