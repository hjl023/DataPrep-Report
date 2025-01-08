# 데이터 전처리 및 비교 처리 코드 설명

이 코드는 **Spark**를 사용하여 데이터의 **전처리**와 **변경 사항 비교**를 수행하는 과정입니다. 데이터 컬럼을 변경하고 전처리를 적용한 후, 원본 데이터와 변경된 데이터를 비교하여 **변동 행수**와 **변경된 컬럼**을 추적합니다. 이 과정에서 최종적으로 **변경된 데이터**를 보고서 형식으로 출력합니다.

## 주요 기능

1. **컬럼 이름 변경 및 전처리**  
   `newTableColumns`에 정의된 컬럼 이름을 기존 컬럼명에 맞춰 변경하며, `StringType` 컬럼에 대해 전처리를 수행합니다.

2. **전처리된 데이터 저장**  
   전처리된 데이터를 Delta 형식으로 지정된 `outputDataPath` 경로에 저장합니다.

3. **변경된 데이터 추적**  
   원본 데이터와 전처리된 데이터를 `id_column`을 기준으로 비교하여 변동된 행들을 추적합니다.

4. **변경된 컬럼 비교**  
   `before`와 `after` 데이터를 비교하여 변경된 컬럼을 추출하고, 그 결과를 보고서 형식으로 출력합니다.

## 코드 흐름

1. **UDF 정의**  
   전처리 함수를 호출하는 `spark_udf`를 정의하여 `StringType` 컬럼에 전처리를 적용합니다.

2. **컬럼 이름 변경**  
   `newTableColumns`에서 제공되는 컬럼 목록을 통해 컬럼 이름을 변경하고, 변경된 컬럼 목록을 생성합니다.

3. **전처리 수행**  
   변경된 컬럼에 대해 `spark_udf`를 사용하여 전처리를 수행합니다.

4. **데이터 저장**  
   전처리된 데이터를 새로운 파일 경로에 Delta 형식으로 저장합니다.

5. **데이터 비교**  
   원본 데이터와 전처리된 데이터를 `id_column`을 기준으로 비교하여 변동된 행들을 추적합니다.

6. **변경된 컬럼 추적**  
   `before`와 `after`의 값을 비교하여 변경된 컬럼을 추출하고, 이를 바탕으로 최종 보고서를 생성합니다.

## 사용 방법

1. **전처리 및 저장**
   ```python
   df_processed = df.select(*target_rename_col_list)
   df_processed.write.format("delta").save(outputDataPath)
   ```

2. **변경된 컬럼 및 변동 행수 확인**
   ```python
   # 변경된 컬럼 추적 및 변동 행수 계산
   changed_columns = []
   changed_ids = set()

   # 보고서 출력
   joined_df.show()
   ```


      ![image](https://github.com/user-attachments/assets/0fcf5166-753d-4c37-a575-4bd40085c095)



## 예외 처리

각 단계에서 발생할 수 있는 예외를 처리하여 오류 메시지를 출력하고, 데이터 처리 흐름이 중단되지 않도록 합니다.


## 실제 수행 방식
WSL2 활성화
- PoewerShell(관리자 권한) 실행
- dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart 입력
- dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart 입력
- 재부팅 (필수)
- PoewerShell(관리자 권한) 실행
- wsl 입력 : Linux용 하위 시스템에 배포가 설치 되어있지 않습니다.Ubuntu 설치
- Microsoft Store에서 Ubuntu 22.04.3 LTS 다운WSL2 리눅스 커널 업데이트
- https://wslstorestorage.blob.core.windows.net/wslblob/wsl_update_x64.msi
- 다운로드후 설치하여 SWL 리눅스 커널 업데이트 하기.Ubuntu 유저이름과 비밀번호 설정
- Ubuntu 22.04.3 LTS를 실행하여 설치가 되고 유저아이디와 비밀 번호 설정.Ubuntu 버전 확인
- PoewerShell(관리자 권한) 실행
- wsl -l -v 입력, version이 2
- wsl -l -v 입력, version이 1인 경우,

-   wsl --set-version Ubuntu-22.04 입력
-  wsl --set-default-version 2 입력Docker 설치 하기


- https://www.docker.com/products/docker-desktop/ 본인 환경에 맞는 도커 설치
- 옵션 'Use WSL 2 instead of Hyeper-V(recommended), 'Add shortcut to desktop'을 선택후 다운로드Docker 버전 확인
- PoewerShell 실행
- docker -v 입력 후 버전 확인
