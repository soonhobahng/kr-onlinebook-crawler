from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import csv, datetime, shutil, os, boto3, re
from openpyxl import Workbook

now = datetime.datetime.now()

def yes24data(school, sUrl):
    book = Workbook()
    yes24Url = 'http://www.yes24.com'
    urllist = []
    excelList = []

    for page in range(1,51,1):
        url = urlopen(sUrl + str(page))
        # print(highSchoolUrl + str(page))
        bsObject = BeautifulSoup(url, "html.parser")


        for cover in bsObject.find_all("td", {"class":"goodsTxtInfo"}):
            bookurl = cover.select('a')[0].get('href')
            urllist.append(bookurl)

    for index, book_lank_url in enumerate(urllist):
        try:
            html = urlopen(yes24Url + book_lank_url)
            bsObject = BeautifulSoup(html, "html.parser")

            if bsObject.find_all("td", {"class":"txt lastCol"})[2].text == '\r\n                            YES24 배송\r\n                        ':
                isbn = ''
            else:
                isbn = bsObject.find_all("td", {"class":"txt lastCol"})[2].text

            date = bsObject.find_all("td", {"class": "txt lastCol"})[0].text
            commodityNo = book_lank_url.split('/')[3]
            commodityName = bsObject.find("h2", {"class": "gd_name"}).text
            price1 = bsObject.find_all("em", {"class": "yes_m"})[0].text
            price2 = bsObject.find_all("em", {"class": "yes_m"})[1].text
            yesPoint = bsObject.find("ul", {"class": "gd_infoLi"}).select('li')[0].text
            auth = bsObject.find("span", {"class": "gd_auth"}).text
            if re.split(('\n|\r|원'), auth)[1] == '':
                reAuth = re.split(('\n|\r'), auth)[2].strip()
            else:
                reAuth = re.split(('\n|\r'), auth)[1].strip()
            pub = bsObject.find("span", {"class": "gd_pub"}).select('a')[0].text
            sellNum = bsObject.find("span", {"class": "gd_sellNum"}).text

            excelData = (strNow, index + 1, isbn, commodityNo, commodityName, price1.split('원')[0], price2,
                       yesPoint.split('원')[0].strip(), reAuth, pub, sellNum.split(' ')[17], date)

            categoryfull = bsObject.find_all("ul", {"class": "yesAlertLi"})[3].text
            categorys = categoryfull.split('\xa0')[-1:]
            category = categorys[0].split('\n')
            categoryList = []

            if category[1] == '국내도서':

                for i in range(1, len(category), 2):
                    categoryList.append(category[i])

            elif category[1] == '중고샵':
                categorys = categoryfull.split('\xa0')[1]
                category = categorys.split('\n')
                for i in range(1, len(category), 2):
                    categoryList.append(category[i])

            else:
                categoryfull = bsObject.find_all("ul", {"class": "yesAlertLi"})[2].text
                categorys = categoryfull.split('\xa0')[-1:]
                category = categorys[0].split('\n')
                for i in range(1, len(category), 2):
                    categoryList.append(category[i])
            # wf.writerow(csvData)
            categoryTuple = tuple(categoryList)
            print(categoryTuple)

            totalData = excelData + categoryTuple
            excelList.append(totalData)
            print(totalData)

        except:
            pass

    # f.close()
    sheet = book .active
    sheet.append(('크롤링일', '순위', 'ISBN', '상품번호', '상품명', '정가', '판매가', 'YES포인트', '저자', '출판사', '판매지수', '출간일', '분류'))
    for row in excelList:
        sheet.append(row)
        print(row)

    # book.save('/home/ec2-user/yes24crawling/data/'+school+'yes24crawling.xlsx')
    book.save('/home/ec2-user/git/onlinebookcrawler/data/' + school + 'yes24crawling.xlsx')


if __name__ == "__main__":
    files = ['elementyes24crawling.xlsx', 'middleyes24crawling.xlsx', 'highyes24crawling.xlsx']
    urlList = ['http://www.yes24.com/24/category/bestseller?CategoryNumber=001001013003&sumgb=08&PageNumber=',
               'http://www.yes24.com/24/category/bestseller?CategoryNumber=001001013002&sumgb=08&PageNumber=',
               'http://www.yes24.com/24/category/bestseller?CategoryNumber=001001044&sumgb=08&PageNumber=']
    schoolList = ['high', 'middle', 'element']
    strNow = now.strftime('%y%m%d')

    # Yes24 데이터 수집 진행중
    # 2019.12.10 방순호
    for i in range(3):
        yes24data(schoolList[i], urlList[i])

    # AWS S3에 저장
    s3 = boto3.client('s3')
    bucket = 'onlinebook-crawling'
    storageaddr = '<Your S3 Path>'

    for file in files:
        s3File = 'pastdata/' + strNow + file
        s3.upload_file(storageaddr + '/data/' + file, bucket, file, ExtraArgs={'ACL': "public-read"})
        s3.upload_file(storageaddr + '/data/' + file, bucket, s3File, ExtraArgs={'ACL': "public-read"})
