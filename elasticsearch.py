import json
import logging
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.DEBUG,
    filename='elasticlog.log',
    format='%(asctime)s, %(levelname)s, %(message)s',
)

INDEX_EXAMPLE = 'windc'


class SMOSBase:

    SCHEME = 'https'
    HOST = ''
    PORT = '9200'
    USERNAME = ''
    PASSWORD = ''

    @classmethod
    def get_base_url(cls):
        return '{}://{}:{}'.format(
            cls.SCHEME, cls.HOST, cls.PORT
            )

    @classmethod
    def get_session(cls):
        with requests.Session() as session:
            session.verify = False
            session.auth = (cls.USERNAME, cls.PASSWORD)
        return session


class SMOS(SMOSBase):
    CURRENT_SESSION = SMOSBase.get_session()

    def get_indices(self, index):
        params = {
            'h': 'index',
            'format': 'json'
        }
        resp = self.CURRENT_SESSION.get(
            self.get_base_url() + f'/_cat/indices/{index}-*', params=params
            ).json()
        indxs = [item['index'] for item in resp]
        logging.info(f'Запрос индексов: {indxs}')
        return indxs

    def get_id(self, part_of_index):
        params = {
            'format': 'json',
            'stored_fields': ['',],
        }
        response = self.CURRENT_SESSION.get(
            self.get_base_url() + f'/{part_of_index}/_search',
            params=params
            )
        response = response.json()['hits']['hits']
        ids = [_['_id'] for _ in response]
        logging.info(f'Запрос `ids` у {part_of_index}: {ids}')
        return ids

    def get_doc(self, part_of_index, id):
        params = {
            'format': 'json',
            'filter_path': '_index,_id,_source.host.os.platform'
        }
        response = self.CURRENT_SESSION.get(
            self.get_base_url() + f'/{part_of_index}/_doc/{id}', params=params
        ).json()
        logging.info(f'Информация в `doc`: {response}')
        return response

    def delete_field(self, part_of_index, id):
        headers = {
            'Content-Type': 'application/json',
        }
        payload = {
            "script": "ctx._source['host']['os'].remove('platform')"
        }
        payload = json.dumps(payload)
        resp = self.CURRENT_SESSION.post(
            self.get_base_url() + f'/{part_of_index}/_update/{id}',
            headers=headers, data=payload
        ).json()
        logging.info(
            f'Информация об удалении: {resp}'
        )
        return resp


def main():
    smos = SMOS()
    indices = smos.get_indices(INDEX_EXAMPLE)
    index_ids_dict = {}
    flag = False
    for index in indices:
        index_ids_dict[index] = smos.get_id(index)
    for index_, ids_ in index_ids_dict.items():
        for id_ in ids_:
            try:
                platform_flag = (
                    smos.get_doc(index_, id_)
                    ['_source']['host']['os']['platform']
                )
            except KeyError:
                logging.error('Отсутствие `platform` в ответе API')
                pass
            if platform_flag:
                print(index_, id_)
                smos.delete_field(index_, id_)
                flag = True
                smos.get_doc(index_, id_)
                break
        if flag:
            break


if __name__ == '__main__':
    main()
