

select s.id, s.age, dbi.ident as sex
from subject s
join db__idents dbi on s.sex = dbi.db__id
