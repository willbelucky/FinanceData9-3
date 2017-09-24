from datetime import datetime
import requests
import pandas as pd


def get_dart_financial_statements(code, start_date=datetime(1900, 1, 1), end_date=datetime(2100, 1, 1)):
    """
    Get financial statements from start_date to end_date by scrapping DART.

    :param code: string with 6 length number.
    :param start_date: datetime
    :param end_date: datetime
    :return:
    """
    pass


def get_reception_number(code, start_date=datetime(1900, 1, 1), end_date=datetime(2100, 1, 1), page_number=1):
    """
    Get reception numbers(rcpNo) from start_date to end_date
    by scrapping `search_by_company_page<https://dart.fss.or.kr/dsab001/main.do#>_`

    :param code: string with 6 length number.
    :param start_date: datetime
    :param end_date: datetime
    :return:
    """
    # STEP 01: Open a search result page.
    """
    ========    ===========================================================
    요청변수	    설명
    ========    ===========================================================
    auth	    발급받은 인증키(40자리)(필수)
    crp_cd	    공시대상회사의 종목코드(상장사:숫자 6자리) 또는 고유번호(기타법인:숫자 8자리)
    end_dt	    검색종료 접수일자(YYYYMMDD) : 없으면 당일
    start_dt	검색시작 접수일자(YYYYMMDD) : 없으면 end_dt
                crp_cd가 없는 경우 검색기간은 3개월로 제한
    fin_rpt 	최종보고서만 검색여부(Y or N) 기본값 : N
                (정정이 있는 경우 최종정정만 검색)
    dsp_tp	    정기공시(A), 주요사항보고(B), 발행공시(C), 지분공시(D), 기타공시(E),
                외부감사관련(F), 펀드공시(G), 자산유동화(H), 거래소공시(I), 공정위공시(J)
    bsn_tp	    정기공시(5), 주요사항보고(3), 발행공시(11), 지분공시(4), 기타공시(9),
                외부감사관련(3), 펀드공시(3), 자산유동화(6), 거래소공시(6), 공정위공시(5)
                (상세 유형 참조)
    sort	    접수일자(date), 회사명(crp), 보고서명(rpt) 기본값 : date
    series	    오름차순(asc), 내림차순(desc) 기본값 : desc
    page_no	    페이지 번호(1~n) 기본값 : 1
    page_set	페이지당 건수(1~100) 기본값 : 10, 최대값 : 100
    callback	콜백함수명(JSONP용)
    ========    ===========================================================
    """
    request_url = 'http://dart.fss.or.kr/api/search.json'
    auth = 'cb6408b5933c098a5489850994e7354006154d4a'
    end_dt = end_date.strftime("%Y%m%d")
    start_dt = start_date.strftime("%Y%m%d")
    form_data = {
        'auth': auth,
        'crp_cd': '00' + code,
        'start_dt': start_dt,
        'end_dt': end_dt,
        'fin_rpt': 'Y',
        'bsn_tp': ['A001', 'A002', 'A003'],
        'series': 'asc',
        'page_no': page_number,
        'page_set': 100,
    }
    df = pd.read_json(requests.post(request_url, form_data).content)

    return df


def get_document_number(reception_number):
    pass


if __name__ == "__main__":
    print(get_reception_number('126380'))
