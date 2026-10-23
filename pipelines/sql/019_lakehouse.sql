-- 7a: Iceberg 카탈로그(PyIceberg SqlCatalog)가 쓰는 스키마.
-- 카탈로그는 "테이블 이름 → 지금 메타데이터 파일 위치" 포인터만 Postgres에 둔다. 데이터 파일·메타데이터·스냅샷 기록은
-- lake 버킷의 iceberg/ 아래에 있다. 포인터 갱신은 DB 트랜잭션이라 두 작성자가 동시에 커밋해도 하나만 이긴다.
-- 표(iceberg_tables, iceberg_namespace_properties)는 PyIceberg가 처음 연결할 때 이 스키마 안에 만든다.
CREATE SCHEMA lakehouse;
