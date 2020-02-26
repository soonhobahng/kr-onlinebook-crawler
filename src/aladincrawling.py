from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import csv, datetime, shutil, os, boto3, re
from openpyxl import Workbook

now = datetime.datetime.now()

# 알라딘 데이터 추출
# 2020.02.19 방순호
# school : elementary(초등), middle(중등), high(고등)
# surl : 베스트셀러 리스트 페이지 URL
# csvurl : 베스트셀러 리스트 CSV 다운로드 URL

def aladindata(school, surl, csvurl):
    book = Workbook()
    urllist = []
    csvList = []
    excelList = []
    tmpExcelList = []

    # csv file download
    csvfile = urlopen(csvurl).read().decode('cp949')
    # 1행씩 나누어 배열에 저장
    csvfile = csvfile.splitlines()
    for row in csvfile:
        if row:
            # 실제 데이터만 추출. CSV 파일 마지막 줄에 알라딘 표어 제거
            csvList.append(row)
        else:
            break

    added_row = []
    index = 0
    for row in csvList:
        if index > 0:
            # 각 행을 배열에 저장
            onerow = row #.split(', ')
            # 1행의 각 필드 분리
            contents = onerow.split('","')
            # 필드에 \r\n 들어있는 파일이 있어 예외처리
            if len(contents) > 14 :
                tmpExcelList.append(contents)
            elif len(added_row) > 0:
                added_row.pop()
                new_row = added_row + contents
                tmpExcelList.append(new_row)
                added_row = []
            else:
                added_row = contents
        index = index + 1

    # 베스트셀러 페이지의 책 링크 수집
    # 책 분류 정보를 수집하기 위함
    for page in range(1, 21, 1):
        url = urlopen(surl + str(page))
        bsObject = BeautifulSoup(url, "html.parser")
        for cover in bsObject.find_all("a", {"class": "bo3"}):
            bookurl = cover.get('href')
            urllist.append(bookurl)

    # 수집한 URL에서 분류 목록 추출
    for index, book_rank_url in enumerate(urllist):
        try:
            html = urlopen(book_rank_url)
            bsObject = BeautifulSoup(html, "html.parser")

            categoryfull = bsObject.find_all("div", {"class": "conts_info_list2"})
            category_columns = categoryfull[0].select("li")[0].find_all("a")
            categories = []

            for category_column in category_columns:
                if category_column.text != '접기':
                    categories.append(category_column.text)

            # 첫 필드와 끝 필드의 double quot 제거
            row0 = tmpExcelList[index][0].replace('"','')
            row14 = tmpExcelList[index][14].replace('"','')
            itemid_idx = book_rank_url.find('=')
            # 분류 목록 생성
            categoryTuple = tuple(categories)

            # 엑셀 행 생성
            excelData = (strNow, row0, tmpExcelList[index][3], book_rank_url[itemid_idx+1:], tmpExcelList[index][2], tmpExcelList[index][8], tmpExcelList[index][9], tmpExcelList[index][12], tmpExcelList[index][7], tmpExcelList[index][6], row14, tmpExcelList[index][13])
            excelList.append(excelData + categoryTuple)
        except:
            pass

    # Excel file 생성
    sheet = book.active
    sheet.append(('크롤링일', '순위', 'ISBN', '상품번호', '상품명', '정가', '판매가', '포인트', '저자', '출판사', '판매지수', '출간일', '분류'))
    for row in excelList:
        print(row)
        sheet.append(row)

    book.save('./data/' + school + 'aladincrawling.xlsx')

if __name__ == "__main__":
    files = ['elementaladincrawling.xlsx', 'middlealadincrawling.xlsx', 'highaladincrawling.xlsx']
    strNow = now.strftime('%y%m%d')

    # 알라딘 데이터 수집 완료
    # 2019.12.10 방순호
    schoolList = ['high', 'middle', 'element']
    aladinUrls = ['https://www.aladin.co.kr/shop/common/wbest.aspx?BestType=Bestseller&BranchType=1&CID=76001&cnt=1000&SortOrder=1&page=',
                  'https://www.aladin.co.kr/shop/common/wbest.aspx?BestType=Bestseller&BranchType=1&CID=76000&cnt=1000&SortOrder=1&page=',
                  'https://www.aladin.co.kr/shop/common/wbest.aspx?BestType=Bestseller&BranchType=1&CID=50246&cnt=1000&SortOrder=1&page=']
    aladinCSVUrls = ['https://www.aladin.co.kr/shop/common/wbest_excel.aspx?BestType=Bestseller&BranchType=1&CID=76001',
                  'https://www.aladin.co.kr/shop/common/wbest_excel.aspx?BestType=Bestseller&BranchType=1&CID=76000',
                  'https://www.aladin.co.kr/shop/common/wbest_excel.aspx?BestType=Bestseller&BranchType=1&CID=50246']
    for i in range(3):
        aladindata(schoolList[i], aladinUrls[i], aladinCSVUrls[i])

    # AWS S3에 저장
    s3 = boto3.client('s3')
    bucket = 'onlinebook-crawling'
    storageaddr = '<Your S3 Path>'

    for file in files:
        s3File = 'pastdata/' + strNow + file
        s3.upload_file(storageaddr + '/data/' + file, bucket, file, ExtraArgs={'ACL': "public-read"})
        s3.upload_file(storageaddr + '/data/' + file, bucket, s3File, ExtraArgs={'ACL': "public-read"})
