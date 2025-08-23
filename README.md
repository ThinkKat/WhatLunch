# Softeer6th - team4 점심뭐먹지 

## 문제


## 해결 방법  


## ETL 파이프라인


## 팀원 소개

<br/>

<div align="center">
<table>
<th>팀원</th>
    <th> 노선우 <a href="https://github.com/SsunyR"><br/><img src="https://img.shields.io/badge/Github-181717?style=flat-square&logo=Github&logoColor=white"/><a></th>
	  <th> 김의진 <a href="https://github.com/uijinee"><br/><img src="https://img.shields.io/badge/Github-181717?style=flat-square&logo=Github&logoColor=white"/></a></th>
    <th> 조원영 <a href="https://github.com/ThinkKat"><br/><img src="https://img.shields.io/badge/Github-181717?style=flat-square&logo=Github&logoColor=white"/></a></th>
  </table>
</div>
<br />
<br />


---

## 프로젝트 정보

- 기간: 2025.08.04 ~ 2025.08.30  


---
# Git Commit Message 규칙 (팀 공용)

## 1. 기본 구조

```
<type>(<scope>): <subject>

<body>
<footer>
```

* **type** : 커밋의 성격 (필수)
* **scope** : 변경된 영역/모듈 (선택)
* **subject** : 한 줄 요약, 마침표 X, 명령문 사용
* **body** : 상세 설명 (선택, 여러 줄 가능)
* **footer** : 관련 이슈 번호, 브레이킹 체인지 표시 (선택)

---

## 2. Type 종류

| type       | 설명                       |
| ---------- | ------------------------ |
| `feat`     | 새로운 기능 추가                |
| `fix`      | 버그 수정                    |
| `docs`     | 문서 변경 (README, 주석 등)     |
| `style`    | 코드 포맷팅/세미콜론 등, 로직 변경 없음  |
| `refactor` | 코드 리팩터링 (동작 변화 없음)       |
| `perf`     | 성능 개선                    |
| `test`     | 테스트 코드 추가/수정             |
| `chore`    | 빌드/패키지 설정, CI/CD 등 기타 변경 |
| `revert`   | 이전 커밋 되돌리기               |

---

## 3. 작성 규칙

* subject는 **50자 이내**, 명령형 어조로 작성

* `Add` 대신 `add` (소문자 시작 권장)
  
* 영어/한글 모두 가능 (type은 영어 유지)
  
* 한 커밋에는 **한 가지 변경사항만** 포함
  
* 이슈 트래킹 시 footer에 `Closes #이슈번호` 또는 `Refs #이슈번호`

---

## 4. 예시

```
feat(parser): 차량 상세 파싱 로직 추가

fix(scraper): ReadTimeout 발생 시 재시도 로직 보완

docs: README.md에 실행 방법 및 커밋 규칙 추가

refactor: fetch_list_urls 함수 구조 단순화

chore: requirements.txt에 ijson 추가

feat(crawler): 차량 경매 크롤링 CLI 옵션 추가

Closes #12
```
