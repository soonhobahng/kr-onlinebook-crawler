from urllib.request import Request, urlopen
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import csv, datetime, shutil, os, boto3, re
from botocore.exceptions import ClientError
from openpyxl import Workbook
import json

now = datetime.datetime.now()

def interparkdata(school):
    book = Workbook()
    interparkUrl = 'http://book.interpark.com/display/collectlist.do?_method=BestsellerHourNewWeekList201605_xml'
    urllist = []
    excelList = []
    schoolNm = 'middle'
    categoryNm = '중등학습서'

    if (school == '028040'):
        schoolNm = 'high'
        categoryNm = '고등학습서'
    elif (school == '028024'):
        schoolNm = 'element'
        categoryNm = '초등학습서'

    params = {}
    params['cltTp'] = '01'
    params['bestTp'] = '1'
    params['dispNo'] = school
    params['clickCnb'] = 'N'

    for page in range(1, 16, 1):
        try:
            params['page'] = page
            index = (page - 1) * 15

            request = Request(interparkUrl, urlencode(params).encode())
            request.get_method = lambda: 'POST'
            contents = urlopen(request).read().decode('cp949')
            contents = contents.replace('(', '').replace(')', '')
            jsonObj = json.loads(contents)

            bookrank = jsonObj['returnObj']['BOOK_K']
            pricelist = jsonObj['returnObj']['priceList']

            if(bookrank):
                for onebook in bookrank:
                    index = index + 1
                    commodityNo = onebook['prdNo']
                    commodityName = onebook['prdNm']
                    reAuth = onebook['author']
                    pub = onebook['hdelvMafcEntrNm']
                    isbn = ''
                    pubdate = ''

                    for oneprice in pricelist:
                        if(oneprice['prdNo'] == commodityNo):
                            sellnum = oneprice['prdIdxVal']
                            price1 = oneprice['mktPr']
                            price2 = oneprice['saleUnitcost']
                            blcpoint = oneprice['blcPoint']

                    try:
                        html = urlopen('http://book.interpark.com' + onebook['linkUrl'])
                        book_detail = BeautifulSoup(html, "html.parser")
                        book_info = book_detail.find_all('ul', {"class" : 'bInfo_txt'})
                        book_columns = book_info[0].find_all('li')

                        for book_column in book_columns:
                            book_column_txt = book_column.text
                            if book_column_txt.find('발행') > -1:
                                pubdate = book_column_txt[5:]

                            if book_column_txt.find('ISBN') > -1:
                                isbn = book_column_txt[6:]

                        category_info = book_detail.find_all('ul',{"class": "classFiedList"})
                        category_columns = category_info[0].find_all('a')
                        categories = []

                        for category_column in category_columns:
                            if category_column.text != 'Home' and category_column.text != '도서':
                                categories.append(category_column.text)

                    except:
                        pass

                    categoryTuple = tuple(categories)

                    excelData = (strNow, index, isbn, commodityNo, commodityName, price1, price2,
                                 blcpoint, reAuth, pub, sellnum, pubdate)
                    excelList.append(excelData + categoryTuple)
        except:
            pass

    sheet = book.active
    sheet.append(('크롤링일', '순위', 'ISBN', '상품번호', '상품명', '정가', '판매가', '포인트', '저자', '출판사', '판매지수', '출간일', '분류'))
    for row in excelList:
        sheet.append(row)
        print(row)

    book.save('./data/' + schoolNm + 'interparkcrawling.xlsx')


if __name__ == "__main__":
    files = ['elementinterparkcrawling.xlsx', 'middleinterparkcrawling.xlsx', 'highinterparkcrawling.xlsx']
    strNow = now.strftime('%y%m%d')

    # 인터파크 데이터 수집
    schoolList = ['028024', '028043', '028040']
    for i in range(3):
        interparkdata(schoolList[i])

    # AWS S3에 저장
    s3 = boto3.client('s3')
    bucket = 'onlinebook-crawling'
    storageaddr = '<Your S3 Path>'

    for file in files:
        s3File = 'pastdata/'+ strNow +file
        s3.upload_file(storageaddr + '/data/' + file, bucket, file, ExtraArgs={'ACL':"public-read"})
        s3.upload_file(storageaddr + '/data/' + file, bucket, s3File, ExtraArgs={'ACL': "public-read"})
