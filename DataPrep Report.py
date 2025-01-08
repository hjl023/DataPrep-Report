import logging

import traceback

from pyspark.sql.functions import col, udf, coalesce, trim, lower

from pyspark.sql.types import StringType

from delta import *




from pyspark.sql import SparkSession

from spark.spark_session import spark_session

from utils.requests_callback_utils import RequestCallbackClass

from prep.param_class import PrepParamClass, CallbackParamObject

import pyspark.sql.functions as F




################## 전처리 함수 관련 start #################

import html2text

import re

import html

from bs4 import BeautifulSoup

from typing import List, Optional




# UDF로 등록할 함수를 외부 모듈에서 선언하여 사용하는 경우 에러 발생. 추후 테스트 필요




# from prep.pyspark_prep import (

#     remove_special_char

#     ,remove_html_tag

#     ,remove_outlier

#     ,filtering_personal_info

#     ,remove_space_tab_newline

#     ,remove_duplicate_row

#     ,make_compat

# )




# from utils.korean_utils import (

#     PHONE_NUMBER_PATTERN,

#     CARD_PATTERN,

#     RESIDENT_NUMBER_PATTERN,

#     BANK_ACCOUNT_PATTERN

# )




# from utils.lang_agnostic_utils import (

#     EMAIL_END_TOKEN,

#     EMAIL_PATTERN,

#     BANK_ACCOUNT_PATTERN,

#     EMAIL_START_TOKEN,

#     HTML_REMOVE,

#     HTML_SPLIT,

#     URL_END_TOKEN,

#     URL_PATTERN,

#     URL_START_TOKEN,

#     replace_with_delete,

#     replace_with_token,

#     replace_with_row_delete

# )

################## 전처리 함수 관련 end #################






logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s: %(message)s', datefmt='%Y-%m-%dT%H:%M:%S%z')

logger = logging.getLogger(__name__)






class PreprocessJob(object):

    """

    datalake-pyspark preprocess job class

    """

    

    # def __init__(self):

        # logger.info("PreprocessJob Class init.")




    def prep_job(self, param: PrepParamClass, callbackAt: bool=True):    

        """

        데이터 전처리 타입에 따라 해당 기능을 수행한다.

        param : PrepParamClass

        callbackAt : 콜백 수행 여부(bool)

        """

        ####################################################  korean_utils ##################################################

        # PHONE NUMBER

        PHONE_NUMBER_PATTERN = re.compile(

            r"([0-9]{2,3}\s*-\s*[0-9]{3,4}\s*-\s*[0-9]{4}\s*[~∼,-]\s*[0-9]{1,4})|"

            r"([0-9]{3}\s*-\s*[0-9]{4}\s*[~∼,-]\s*[0-9]{1,4})|"

            r"([0-9]{2,3}\s*-\s*[0-9]{3,4}\s*-\s*[0-9]{4})|"

            r"([0-9]{2,3}\s*[0-9]{3,4}\s*[0-9]{4})"

        )




        # CARD

        CARD_PATTERN = re.compile(r"([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4})")




        # RESIDENT

        RESIDENT_NUMBER_PATTERN = re.compile(r"([0-9]{6}\s*-\s*[0-9]{7})")




        # BANK ACCOUNT

        BANK_ACCOUNT_PATTERN = re.compile(

            r"([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # IBK / KB / KEB / Hana

            + r"([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})" # KB

            + r"([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2})"  # NH

            + r"([0-9]\d{11})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # SHINHAN / SHINHYUP / CITI

            + r"([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # WOORI

            + r"([0-9]\d{11})|([0-9]\d{11})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-])"  # DGB

            + r"([0-9]\d{12})|([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2})"  # BNK

            + r"([0-9]\d{10})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # SC

            + r"([0-9]\d{11})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # KBANK

            + r"([0-9]\d{12})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,7})"  # KAKAO

        )






        ####################################################  lang_agnostic_utils ##################################################




        # EMAIL

        EMAIL_PATTERN = re.compile(

            r"[a-z0-9.\-+_]+@[a-z0-9.\-+_]+\.[a-z]+|[a-z0-9.\-+_]+@[a-z0-9.\-+_]+\.[a-z]+\.[a-z]"

        )




        BANK_ACCOUNT_PATTERN = re.compile(

            r"([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # IBK / KB / KEB / Hana

            + r"([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})" # KB

            + r"([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2})"  # NH

            + r"([0-9]\d{11})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # SHINHAN / SHINHYUP / CITI

            + r"([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # WOORI

            + r"([0-9]\d{11})|([0-9]\d{11})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-])"  # DGB

            + r"([0-9]\d{12})|([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2})"  # BNK

            + r"([0-9]\d{10})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # SC

            + r"([0-9]\d{11})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # KBANK

            + r"([0-9]\d{12})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,7})"  # KAKAO

        )




        # 

        HTML_REMOVE = [

            "javascript",

            "/",

            "#",

            "*",

            "![",

            "[!",

            "[(",

            ")]",

            "[]",

            "()",

            ";",

            "뉴스",

            "기사",

            "신문",

            "카카오",

            "네이버",

            "티스토리",

            "무단전재",

            "저작권",

            "재배포",

            "구독",

            "검색어",

            "=",

            "__",

        ]




        HTML_SPLIT = [

            "더보기",

            "더 보기",

        ]




        URL_PATTERN = re.compile(

            r"""\b((?:https?://)?(?:(?:www\.)?(?:[\da-z\.-]+)\.(?:[a-z]{2,6})|(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|(?:(?:[0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:(?:(?::[0-9a-fA-F]{1,4}){1,6})|:(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(?::[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(?:ffff(?::0{1,4}){0,1}:){0,1}(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])|(?:[0-9a-fA-F]{1,4}:){1,4}:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))(?::[0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])?(?:/[\w\.-]*)*/?)\b"""

        )




        URL_START_TOKEN = "<|url_start|>"

        URL_END_TOKEN = "<|url_end|>"

        EMAIL_PATTERN = re.compile(

            r"[a-z0-9.\-+_]+@[a-z0-9.\-+_]+\.[a-z]+|[a-z0-9.\-+_]+@[a-z0-9.\-+_]+\.[a-z]+\.[a-z]"

        )




        EMAIL_START_TOKEN = "<|email_start|>"

        EMAIL_END_TOKEN = "<|email_end|>"






        # 특정 패턴을 찾아 패턴에 해당하는 부분을 제거

        def replace_with_delete(text, pattern):




            found = re.finditer(pattern, text)  

            for match in found: 

                text = text.replace(match.group(), '')

            return text




        # 특정 패턴을 찾아 패턴에 해당하는 부분을 지정된 형식으로 대체한다

        def replace_with_token(text, pattern):




            found = re.finditer(pattern, text)

            for match in found:

                replace = re.sub(r'\w', '#', match.group())  # 모든 문자 및 숫자를 '#'로 대체

                text = text.replace(match.group(), replace)

            return text




        # 특정 패턴을 찾아 패턴에 해당하는 row를 제거

        def replace_with_row_delete(text, pattern):




            lines = text.split('\n')

            new_lines = []

            

            for line in lines:

                found = re.findall(pattern, line)

                if not found:

                    new_lines.append(line)




            return '\n'.join(new_lines)






        #################################################### 전처리 함수 선언 start ##################################################

        

        def remove_special_char(text: str, param: dict):

            """

            특수문자 제거

            remove_select_special_char: str       # 제거 대상 특수문자

            remove_all_special_char: bool = True  # 전체 제거 여부

            """




            remove_select_special_char: str = param['remove_select_special_char']

            remove_all_special_char: bool = param['remove_all_special_char']




            special_char_pattern = ''




            if remove_all_special_char:

                special_char_pattern = r'[^\s\w]'

            else:

                special_char_pattern = '[' + re.escape(remove_select_special_char) + ']'




            text = re.sub(special_char_pattern, ' ', text)

            return text

        




        def remove_special_chars(text: str, remove_type : bool, special_chars_exception: Optional[List[str]] =
None):

            """

            한국어 문장에서 특수 문자를 제거하는 함수 (제거하지 않을 특수문자를 지정한 경우)            

            Args:

                text (str): 특수 문자를 제거할 문장

                remove_type (bool) : 특수 문자 리스트에 포함된 특수문자를 제거할지(False) 제거하지 않을지(True) 결정

                special_chars_exception (Optional[List[str]]): 특수 문자 리스트 (기본값: None)




            Returns:

                str: 특수 문자가 제거된 문장

            """




            # 모든 특수문자 제거

            if special_chars_exception is None:

                special_chars_pattern = re.compile(r'[^\w\s]')

            else:

                # 입력받은 특수문자를 제거 목록에서 제외

                if remove_type == True:

                    str_special_chars_exception = ''.join(special_chars_exception)

                    special_chars_pattern = re.compile(fr"[^ 가-힣a-zA-Z0-9\s{re.escape(str_special_chars_exception)}]")




                # 입력받은 특수문자만 제거

                else:

                    str_special_chars_exception = ''.join(special_chars_exception)

                    special_chars_pattern = re.compile(fr"[{re.escape(str_special_chars_exception)}]")




            # 특수 문자 제거

            cleaned_text = special_chars_pattern.sub(' ', text)




            return cleaned_text        

                




        def filtering_personal_info(text: str, param: dict):

            """

            개인정보필터링

            delete: bool        # 검출된 개인정보만 삭제

            masking: bool        # 검출된 개인정보 마스킹

            row_delete: bool    # 검출된 개인정보 로우 삭제

            """

            delete: bool = param['delete']

            masking: bool = param['masking']

            row_delete: bool = param['row_delete']




            if delete:

                text = replace_with_delete(text, CARD_PATTERN)

                text = replace_with_delete(text, BANK_ACCOUNT_PATTERN)

                text = replace_with_delete(text, PHONE_NUMBER_PATTERN)

                text = replace_with_delete(text, RESIDENT_NUMBER_PATTERN)

                text = replace_with_delete(text, EMAIL_PATTERN)




            if masking:

                text = replace_with_token(text, CARD_PATTERN)

                text = replace_with_token(text, BANK_ACCOUNT_PATTERN)

                text = replace_with_token(text, PHONE_NUMBER_PATTERN)

                text = replace_with_token(text, RESIDENT_NUMBER_PATTERN)

                text = replace_with_token(text, EMAIL_PATTERN)

                

            if row_delete:

                text = replace_with_row_delete(text, CARD_PATTERN)

                text = replace_with_row_delete(text, BANK_ACCOUNT_PATTERN)

                text = replace_with_row_delete(text, PHONE_NUMBER_PATTERN)

                text = replace_with_row_delete(text, RESIDENT_NUMBER_PATTERN)

                text = replace_with_row_delete(text, EMAIL_PATTERN)




            return text






        def remove_outlier(text: str, param: dict):

            """

            아웃라이어 처리

            min_doc_len: int  아웃라이어 최소값

            max_doc_len: int  아웃라이어 최대값

            """




            min_doc_len: int  = param['min_doc_len']

            max_doc_len: int  = param['max_doc_len']




            word_count = len(text.strip())

            

            if min_doc_len <= word_count <= max_doc_len:

                return text

            

            return ''

        




        def clean_space(text):

            """

            중복 공백 및 개행 제거

            """




            text = re.sub("[\r\n\f\v\t]", " ", text)

            while "  " in text:

                text = text.replace("  ", " ")

                

            return text.strip()

        

        def clean_space_fn(text:str, param: dict):

            return clean_space(text)






        def preprocess_html_tags(text:str, param: dict):

            """

            html 소스 및 태그 제거

            """




            # html 태그 및 소스 제거 전 전처리

            def process_html_and_uri_text(text: str):

                text = html.unescape(text) # escape처리된 html 태그를 다시 특수문자로 변환

                text = re.sub(r"<\s*/?\s*br\s*/?\s*>", "\n", text) # <br>은 개행으로 변환

                text = re.sub(r"<\s*/?\s*BR\s*/?\s*>", "\n", text) # <BR>은 개행으로 변환

                return text




            # html 소스 및 태그 제거

            def remove_html_tags(text: str):

                if bool(BeautifulSoup(text, "html.parser").find()):

                    try:

                        processed_html = html2text.html2text(text)

                        # print(processed_html)

                    except AssertionError:

                        processed_html = text

                    

                    text = processed_html

                    text = clean_space(text)

                    # print(text)




                    for pattern in [

                        URL_PATTERN,

                        URL_START_TOKEN,

                        URL_END_TOKEN,

                        EMAIL_PATTERN,

                        EMAIL_START_TOKEN,

                        EMAIL_END_TOKEN

                    ]:

                        text = re.sub(pattern, "", text)




                    sents = re.split(r"(?<=[.!?])\s", text)




                    filtered_sents = []

                    

                    for sent in sents:

                        add = True

                        for symbol in HTML_REMOVE:

                            if symbol in sent:

                                add = False

                                break




                        if add is True:

                            for symbol in HTML_SPLIT:

                                sent = sent.split(symbol)[0]

                            filtered_sents.append(sent)

                    # print(filtered_sents)

                    text = " ".join(filtered_sents)

                    text = clean_space(text)

                    text = text.replace(" !", "")

                return text

            

            return remove_html_tags(process_html_and_uri_text(text))






        ################################################### 전처리 함수 선언 end ##################################################

      

        try:   

            

            storage_path_prepix = "s3a://"        

            storage_path_prepix = ""    # 호출 측에서 full url 셋팅       

            callback_param_object = CallbackParamObject()




            # 필수 파라미터        

            preprocessIdx = param.preprocessIdx

            preprocess_type = param.preprocessType

            inputDataPath = storage_path_prepix + param.inputDataPath

            outputDataPath = storage_path_prepix + param.outputDataPath

            targetTableColumns = param.targetTableColumns

            newTableColumns = param.newTableColumns       

            sourceDataCount = 0 #소스 데이터의 행의 건수

            sourceDataSize = 0  #소스 데이터사이즈(바이트)

            resultDataCount = 0 #결과 데이터의 행의 건수

            resultDataSize = 0 #결과 데이터사이즈(바이트)

            matchedCount =  0 #변동된 행의 수     

            

            with spark_session() as spark:

                df = spark.read.format("delta").load(inputDataPath)

                # 데이터프레임의 행 수 계산

                numRowsOri = df.count()

                #데이터프레임의 바이트 수 계산

                totalSizeOri = df.rdd.map(lambda row: len(str(row))).reduce(lambda a, b: a + b)

                

                #전처리 전 파일에 대한 파라미터 설정

                sourceDataCount = numRowsOri #소스 데이터의 행의 건수

                sourceDataSize = totalSizeOri #소스 데이터사이즈(바이트)

                

                # print("sourceDataCount:",sourceDataCount)

                # print("sourceDataSize:",sourceDataSize)




                # 전처리 호출시 사용될 매개변수 객체

                preprod_param = {}

                spark_udf_fn = None




                ########################## 전처리 별 파라미터 및 함수 설정 #########################




                # 특수문자 제거

                if preprocess_type == 'REMOVE_SPECIAL_CHAR':

                    optionType = param.optionType

                    remove_select_special_char = param.optionContent

                    remove_all_special_char = optionType == "all"




                    preprod_param['remove_select_special_char'] = remove_select_special_char

                    preprod_param['remove_all_special_char'] = remove_all_special_char




                    spark_udf_fn = remove_special_char                 

                    print("Selected preprocess type: REMOVE_SPECIAL_CHAR")

                    print("remove_select_special_char:", remove_select_special_char)

                    print("remove_all_special_char:", remove_all_special_char)




                # HTML 소스 및 태그 제거

                elif preprocess_type == 'REMOVE_HTML_TAG':

                    spark_udf_fn = preprocess_html_tags    # 별도 파라미터 없음

                    print("Selected preprocess type: REMOVE_HTML_TAG")




                # 데이터 아웃라이어 처리 

                elif preprocess_type == 'REMOVE_OUTLIER':                    

                    preprod_param['min_doc_len'] = int(param.optionMin)

                    preprod_param['max_doc_len'] = int(param.optionMax)




                    spark_udf_fn = remove_outlier

                    print("Selected preprocess type: REMOVE_OUTLIER")

                    print("min_doc_len:", preprod_param['min_doc_len'])

                    print("max_doc_len:", preprod_param['max_doc_len'])




                # 개인정보 필터링

                elif preprocess_type == 'FILTERING_PERSONAL_INFO':                    

                    delete = masking = row_delete = False

                    optionType = param.optionType




                    if optionType == "delete":

                        delete = True

                    elif optionType == "masking":

                        masking = True

                    elif optionType == "row_delete":

                        row_delete = True




                    preprod_param['delete'] = delete

                    preprod_param['masking'] = masking

                    preprod_param['row_delete'] = row_delete




                    spark_udf_fn = filtering_personal_info                

                    # print("Selected preprocess type: FILTERING_PERSONAL_INFO")

                    # print("delete:", delete)

                    # print("masking:", masking)

                    # print("row_delete:", row_delete)




                # 공백/탭/개행 제거

                elif preprocess_type == 'REMOVE_SPACE_TAB_NEWLINE':

                    spark_udf_fn = clean_space_fn                    




                # 중복 행 제거 

                elif preprocess_type == 'REMOVE_DUPLICATE_ROW':

                    print("Selected preprocess type: REMOVE_DUPLICATE_ROW")




                # ########################## 전처리 수행 #########################




                # def spark_udf(preprod_param):

                #     return udf(lambda z: spark_udf_fn(z, preprod_param), StringType())                    




                # # 생성 대상 변경 컬럼명 리스트

                # target_rename_col_list = []




                # # 생성 대상 컬럼 확인

                # for each_new_table_column in newTableColumns:

                #     origin_column_name = each_new_table_column.get('originColumnName')

                #     rename_column_name = each_new_table_column.get('columnName')




                #     # 생성 대상 컬림 리스트에 추가

                #     target_rename_col_list.append(rename_column_name)                  

                    

                #     for each_table_column in targetTableColumns:

                #         # 전처리 대상 컬럼

                #         column_name = each_table_column.get('columnName')




                #         # 전처리 대상 컬럼인 경우 전처리 수행

                #         if column_name == origin_column_name:          

                #             # print(f"Applying preprocessing on column '{origin_column_name}' to '{rename_column_name}'")

                #             df = df.withColumn(rename_column_name, spark_udf(preprod_param)(col(origin_column_name)))

                #         else:

                #             df = df.withColumn(rename_column_name, col(origin_column_name))

               #######################################Stringtype에 맞게 전처리 수행############################

                

                def spark_udf(preprod_param):

                    return udf(lambda z: spark_udf_fn(z, preprod_param), StringType())




                # 이름 변경 컬럼 리스트

                target_rename_col_list = []




                # 생성 대상 컬럼 확인 및 이름 변경

                for each_new_table_column in newTableColumns:

                    origin_column_name = each_new_table_column.get('originColumnName')

                    rename_column_name = each_new_table_column.get('columnName')




                    # 생성 대상 컬럼 리스트에 추가

                    target_rename_col_list.append(rename_column_name)                  

                    

                    # 컬럼 이름 변경

                    df = df.withColumnRenamed(origin_column_name, rename_column_name)




                # 전처리 대상 컬럼 확인 및 전처리 수행

                for rename_column_name in target_rename_col_list:

                    # 컬럼의 데이터 타입 확인

                    column_data_type = df.schema[rename_column_name].dataType




                    # 전처리 대상 컬럼이고, 데이터 타입이 StringType인 경우 전처리 수행

                    if isinstance(column_data_type, StringType):

                        # print(f"Applying preprocessing on column '{rename_column_name}'")

                        df = df.withColumn(rename_column_name, spark_udf(preprod_param)(col(rename_column_name)))

                

                ################################# 전처리 파일 생성 ###################################

                                

                # 선택된 컬럼을 대상으로 신규 파일 생성

                print("Saving processed data to output path:", outputDataPath)

                df_processed = df.select(*target_rename_col_list)

                df_processed.write.format("delta").save(outputDataPath)




                # 데이터프레임의 행 수 계산

                numRowsPre = df_processed.count()

                #데이터프레임의 바이트 수 계산

                totalSizePre = df_processed.rdd.map(lambda row: 
len(str(row))).reduce(lambda a, b: a + b)

                

                #전처리 전 파일에 대한 파라미터 설정

                resultDataCount = numRowsPre #결과 데이터의 행의 건수

                resultDataSize = totalSizePre #결과 데이터사이즈(바이트)

            

                ##############################################인덱스 설정######################################

                id_column = '_idx'  # 기본 id 컬럼명 (api에서 값을 주지 않으면 기본값으로 사용)

                user_id_column = 'id'  # 이 변수를 api에서 가져와서 설정

                if user_id_column:

                    id_column = user_id_column




                ###############################데이터로드#########################            

                try:

                    df_before = spark.read.parquet(inputDataPath)

                    df_after = spark.read.parquet(outputDataPath)

                    print("데이터 로드 완료")




                    # id_column이 없으면 기본값으로 설정

                    if id_column not in df_before.columns 
or id_column not in df_after.columns:

                        print(f"{id_column} 컬럼이 없어 {id_column}을 '_idx'로 설정합니다.")

                        id_column = '_idx'

                        if id_column not in df_before.columns
or id_column not in df_after.columns:

                            raise ValueError(f"{id_column} 컬럼도 존재하지 않습니다. 데이터의 index 컬럼을 확인하세요.")




                    ############################매핑#########################################

                    mapping = {}

                    for target_col in targetTableColumns:

                        target_column_name = target_col["columnName"]

                        matched_column = next((new_col for new_col 
in newTableColumns if new_col["originColumnName"] == target_column_name),
None)

                        if matched_column:

                            mapping[target_column_name] = matched_column["columnName"]

                    print("매핑 완료")




                    ##############변동행수 (matchedCount)#############################

                    changed_ids = set()

                    for origin_col, target_col in mapping.items():

                        if origin_col == id_column or target_col == id_column:

                            continue

                        try:

                            df_before_renamed = df_before.select(id_column, col(origin_col).alias(f'{origin_col}_before'))

                            df_after_renamed = df_after.select(id_column, col(target_col).alias(f'{target_col}_after'))

                            df_combined = df_before_renamed.join(df_after_renamed, id_column)

                            changed_id_rows = df_combined.filter(col(f'{origin_col}_before') != col(f'{target_col}_after')).select(id_column).collect()

                            changed_ids.update(row[id_column] for row in changed_id_rows)

                        except Exception as e:

                            print(f"Error processing columns '{origin_col}' and '{target_col}': {e}")




                    matchedCount = len(changed_ids)

                    print(f'변동행수: {matchedCount}')




                    ###############################보고서 출력#########################

                    try:

                        changed_columns = []

                        for origin_col, target_col in mapping.items():

                            if origin_col in df_before.columns and target_col
in df_after.columns:

                                df_before_distinct = df_before.select(origin_col).distinct()

                                df_after_distinct = df_after.select(target_col).distinct()

                                before_set = set(row[0] for row 
in df_before_distinct.collect())

                                after_set = set(row[0] for row in df_after_distinct.collect())

                                if before_set != after_set:

                                    changed_columns.append((origin_col, target_col))




                        if not changed_columns:

                            print("변동된 컬럼이 없습니다.")

                        elif id_column not in df_after.columns:

                            print(f"{id_column} 컬럼이 없어 보고서를 생성할 수 없습니다.")

                        else:

                            # 같은 이름의 컬럼 처리

                            same_name_columns = [col for col in changed_columns
if col[0] == col[1]]

                            for origin_col, target_col in same_name_columns:

                                df_before = df_before.withColumnRenamed(origin_col, f"{origin_col}_before")

                                df_after = df_after.withColumnRenamed(target_col, f"{target_col}_after")




                            # 다른 이름의 컬럼 처리

                            different_name_columns = [col for col in changed_columns
if col[0] != col[1]]

                            for origin_col, target_col in different_name_columns:

                                df_before = df_before.withColumnRenamed(origin_col, f"{origin_col}_before")

                                df_after = df_after.withColumnRenamed(target_col, f"{target_col}_after")




                            # Select renamed columns

                            columns_before = [id_column] + [f"{col}_before" for col, _
in changed_columns]

                            columns_after = [id_column] + [f"{col}_after" for _, col
in changed_columns]




                            df_before_renamed = df_before.select(columns_before)

                            df_after_renamed = df_after.select(columns_after)




                            joined_df = df_before_renamed.join(df_after_renamed, on=id_column, how='inner')




                            # Reordering the final columns

                            final_columns = [id_column] + [item for sublist
in [(f"{col}_before", f"{target_col}_after") for col, target_col 
in changed_columns] for item in sublist]




                            joined_df = joined_df.select(final_columns)




                            # Sort by index in ascending order

                            joined_df = joined_df.orderBy(id_column)




                            # report_outputDataPath = f"{outputDataPath}_report"

                            # joined_df.write.format("delta").save(report_outputDataPath)  #보고서 저장부분

                            joined_df.show()

                            # print("보고서가 저장되었습니다:", report_outputDataPath)

                        print("보고서 출력 완료")

                    except Exception as e:

                        print(f"Error creating or saving the After Only DataFrame(보고서출력): {e}")

                except Exception as e:

                    print(f"Error: {e}")




                ######################## callback #########################                    

            

                # callback_param_object.set_status(200, "OK")

                          

                




        except Exception as e:            

            logger.error(logger.error(traceback.format_exc()))

            callback_param_object.set_status(500, "전처리 중 알수없는 오류가 발생하였습니다.")




        finally:

            # request callback

            if callbackAt:

                callback_param_object.set_result(newTableColumns, sourceDataCount, sourceDataSize, resultDataCount, resultDataSize, matchedCount)

                requestCallbackClass = RequestCallbackClass()

                requestCallbackClass.requests_callback(preprocessIdx, callback_param_object.get_object())

            




