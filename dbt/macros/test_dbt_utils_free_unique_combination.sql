{#
  여러 열의 조합이 유일한지 검사하는 일반 테스트.
  dbt_utils 패키지의 같은 기능을 쓰면 실행 때마다 인터넷에서 패키지를 받아야 해서, 작은 테스트는 직접 둔다.
#}
{% test dbt_utils_free_unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as duplicate_count
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
