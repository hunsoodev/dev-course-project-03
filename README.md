
# Airflow Docker Local Setup


- Docker, Git이 기본적으로 설치되어 있어야함



## 설치 및 실행 방법
**1. Docker Compose 설치 확인**
```shell
$ docker compose version
Docker Compose version v2.22.0-desktop.2
```

만약 Docker Compose를 설치해야 한다면 다음과 같은 명령어를 실행  
```shell
sudo curl -L "https://github.com/docker/compose/releases/download/v2.23.3/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```
[Docker Compose 최신 버전 확인](https://github.com/docker/compose/releases)  
[Install Docker Compose 참고](https://github.com/docker/compose/blob/v1/INSTALL.md)

<br>

**2. 리포지토리 복제**
```shell
git clone https://github.com/hunsoodev/dev-course-project-03.git
cd dev-course-project-03
```
dev-course-project-03 리포지토리를 다운로드 받고 메인폴더로 이동  
여기 있는 dags 폴더가 airflow dags 폴더가 되고 여기 있는 파이썬 파일들이 DAG로 인식됨

</br>

**3. 환경 설정 파일 생성**  
airflow를 실행하기 전에 현재 사용자의 UID를 .env파일에 설정함  
이 작업은 Docker 컨테이너와 호스트 시스템 간의 파일 권한 문제를 해결하기 위해 필요(권장)
```shell
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

```

</br>

**4. Airflow 초기화 (처음 설정할 때만 필요)**  
초기 데이터베이스 구조 설정 및 기본 사용자 생성
```shell
docker-compose up airflow-init
```

</br>

**5. Airflow 서비스 시작**
```shell
docker-compose up -d
```

</br>

**6. Airflow 웹 인터페이스 접근**  
airflow 웹 인터페이스는 http://localhost:8080 에서 접근할 수 있음.
추가 세부사항은 docker-compose.yaml 파일 참고

</br>

**7. 종료 및 정리**  
airflow를 종료하고 모든 컨테이너를 정리하려면 다음과 같은 명령어 사용
```shell
docker-compose down
```



