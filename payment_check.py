import urllib.parse
import json
import asyncio
import concurrent.futures
import argparse
import xml.etree.ElementTree
import itertools

import zkh_xml
import tqdm


CHUNK_SIZE = 500
AB_SERVICE = 'http://it01/SurgF/hs/GG/CalcCheck/'


def read_answer(xml_text, accs_list):
    result = []
    ns = {'soap': 'http://schemas.xmlsoap.org/soap/envelope',
          'ns4': 'http://dom.gosuslugi.ru/schema/integration/base/',
          'ns5': 'http://dom.gosuslugi.ru/schema/integration/account-base/',
          'ns12': 'http://dom.gosuslugi.ru/schema/integration/bills-base/',
          'ns13': 'http://dom.gosuslugi.ru/schema/integration/bills/'}
    try:
        root = xml.etree.ElementTree.fromstring(xml_text)
    except xml.etree.ElementTree.ParseError:
        result.append('===Error=Root parse error=')
        return result, True
    items = []
    try:
        error = root.find(".//ns4:ErrorMessage/ns4:Description", ns).text
        error_code = root.find(".//ns4:ErrorMessage/ns4:ErrorCode", ns).text
        result.append('===Error=' + error + '=')
        for acc in accs_list:
            items.append({"error_code": error_code, "error_message": error})
        return items, False
    except AttributeError:
        pass

    try:
        docs = root.findall('.//ns13:exportPaymentDocResult', ns)
        for doc in docs:
            payment_period = f"{doc.find('.//ns4:Year', ns).text}-{doc.find('.//ns4:Month', ns).text.zfill(2)}-01T00" \
                             f":00:00 "
            if root.find(".//ns4:Error", ns):
                acc_guid = doc.find('.//ns5:AccountGuid', ns).text
                err_code = root.find(".//ns4:Error/ns4:ErrorCode", ns).text
                err_message = root.find(".//ns4:Error/ns4:Description", ns).text
                items.append({"acc_guid": acc_guid, "payment_period": payment_period,
                              "err_code": err_code, "err_message": err_message})
            else:
                acc_guid = doc.find('.//ns5:AccountGuid', ns).text
                uniq_number = doc.find('.//ns12:PaymentDocumentID', ns).text
                items.append({"acc_guid": acc_guid, "payment_period": payment_period, "uniq_number": uniq_number})

    except AttributeError:
        return ['Error'], True
    result_dict = {
        "operation": "payment_write",
        "items": items
    }
    return result_dict, False


def task(guid, accs_list, date, p_bar):
    accs_list = list(accs_list)
    while True:
        xml_req = zkh_xml.payment_export(guid, accs_list, date)
        signed_xml = zkh_xml.sign(xml_req)

        gis_data, error = zkh_xml.gis_data(AB_SERVICE, 'urn:exportPaymentDocumentData', guid, signed_xml,
                                           service_addr=['bills-service', 'BillsAsync'], timeout=10)

        if gis_data == 'Timeout Error':
            continue
        answer, read_error = read_answer(gis_data, accs_list)

        if read_error:
            continue
        ans = json.dumps(answer, ensure_ascii=False)

        zkh_xml.write_ab('http://it01/SurgF/hs/GG/json/', ans)
        p_bar.update()
        break


async def asynchronous_threads(loop, executor, accounts_json, date, p_bar):
    accounts_json = sorted(accounts_json, key=lambda k: k['HOUSEGUID'])

    tasks = [asyncio.ensure_future(loop.run_in_executor(executor, task, house, list(accs), date, p_bar))
             for house, accs in itertools.groupby(accounts_json, key=lambda x: x['HOUSEGUID'])]

    p_bar.total = len(tasks)
    await asyncio.wait(tasks)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('district')
    parser.add_argument('date')

    args = parser.parse_args()

    ab_service_addr = f'{AB_SERVICE}{urllib.parse.quote(args.district)}/0/?YearMonth={args.date}'

    accounts = zkh_xml.get_ab_data(ab_service_addr)
    accounts_json = json.loads(accounts)['Данные']

    pbar = tqdm.tqdm()

    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(1)
    loop.run_until_complete(asynchronous_threads(loop, executor, accounts_json, args.date, pbar))

    loop.close()
    pbar.close()


if __name__ == '__main__':
    main()
