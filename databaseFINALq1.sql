use CH07�d�Ҹ�Ʈw

/*05�i������ : �����������j10%
�ЦC�X�C�@�~�����~�Z�P�Ʀ�]
[��X](*�~��, ���O�W��, ���~�W��, *�P��ƶq, *�P��ƦW, *�Ƶ�����)
[����]
�P��ƦW : �̾ھP��ƶq�A�̰��̬���1�W (�ϥ� dense_rank())
�Ƶ� �Ш̾ڦW���ӵ��w �Ƶ�����
�P��ƦW 1-3 �W : ���Z�P
�P��ƦW 4-6 �W : �Z�P
��L            : �ť�
*/
--�iSolution�j
select year(o.�q�f���) as �~��, ca.���O�W��, p.���~�W��
	 , sum(od.�ƶq) as �P��ƶq
	 , dense_rank() over(partition by year(o.�q�f���) order by sum(od.�ƶq) desc) as ��ƱƦW
	 , case 
	   when dense_rank() over(partition by year(o.�q�f���) order by sum(od.�ƶq) desc) in (1, 2, 3) then '���A�W�`'
	   when dense_rank() over(partition by year(o.�q�f���) order by sum(od.�ƶq) desc) in (4, 5, 6) then '���A�}�n'
	   else '' end as �Ƶ�����
from �q�� as o, �q����� as od, ���~��� as p, ���~���O as ca
where o.�q��s�� = od.�q��s��
  and od.���~�s�� = p.���~�s��
  and p.���O�s�� = ca.���O�s��
group by year(o.�q�f���), ca.���O�W��, p.���~�W��
go


use ��Ʈw�t�δ������i;

select �ɨƦW��, �~��, ���, �W��, ��ƱƦW, ���A
from (select r.�ɨƦW��, year(com.�}�l���) as �~��, r.���, c.�W��
	 , dense_rank() over(partition by year(com.�}�l���) order by isnull(r.���, cr.�ɶ�����) asc) as ��ƱƦW
	 , case 
	   when dense_rank() over(partition by year(com.�}�l���) order by isnull(r.���, cr.�ɶ�����) asc) in (1, 2, 3) then '�}�n'
	   when dense_rank() over(partition by year(com.�}�l���) order by isnull(r.���, cr.�ɶ�����) asc) in (4, 5, 6) then '�@��'
	   else '���z�Q'
	   end as ���A
from ���Z as r, ��� as c, ���� as com, �ɨƳW�w as cr
where r.�ϥΤ�� = c.�W��
	and r.���� = '333'
	and r.�ɨƦW�� = com.�W��
	and com.�W�� = cr.�ɨƦW��
	and cr.���� = '333'
group by year(com.�}�l���), r.�ɨƦW��, r.���, c.�W��, cr.�ɶ�����) as tableA
where ��ƱƦW <= 8;
go




go


select *
from ���Z as r, ���� as com, �ɨƳW�w as cr
where r.���� = '333'
	and r.�ɨƦW�� = com.�W��
	and com.�W�� = cr.�ɨƦW��











update ���
set �t�� = '�©�'
where �t�� = '�թ�'
go

select *
from ���







-- substring(c.����, 1, charindex(',', c.����) - 1)


select c.�W��, count(*)
from ���� as c
	join ��a��� as b on c.�W�� = b.�ɨƦW��
group by c.�W��



select k.�~��, k.�ɨƦW��, convert(numeric(5, 3)
	, t.��a����� / (k.��a�����*1.0)) as ��a����ϥΤ�v
	, k.����
from (select �ɨƦW��, count(�ϥΤ��) as ��a�����
	from (select �ɨƦW��, �ϥΤ��
		from ���Z
		group by �ɨƦW��, �ϥΤ��) as tableB
		group by �ɨƦW��) as t
	join (select comp.�W�� as �ɨƦW��
		, count(r.�ϥΤ��) as ��a�����
		, substring(comp.����, 1, charindex(',', comp.����) - 1) as ����
		, year(comp.�}�l���) as �~��
		from ���� as comp 
			join ���Z as r on r.�ɨƦW�� = comp.�W��
		group by comp.�W��
			, substring(comp.����, 1, charindex(',', comp.����) - 1)
			, year(comp.�}�l���)) as k on t.�ɨƦW�� = k.�ɨƦW��
order by �~�� desc, ��a����ϥΤ�v desc;
go



select distinct comp.�W�� as �ŦX������
from ���� as comp
	left join �ɨƥN�� as r on comp.�W�� = r.�ɨƦW��
	left join �D��� as h on comp.�W�� = h.�ɨƦW��
where substring(comp.����, 1, charindex(',', comp.����) - 1) in ('Kaohsiung', 'Tainan')
	and (r.WCA�N�� like '%[���g�h�B�Ͷv]%'�@
	or h.�D��ζ� like '%�L�ɼ�%')
	

select *
from (
	select top 5 with ties ���, year(com.�}�l���) as �~��, chosen.�ŦX������ as ���ɦW��
		, substring(com.����, 1, charindex(',', com.����) - 1) as �Ҧb����
		, r.�ϥΤ��, 'fast' as �Ʀ�
	from (select distinct comp.�W�� as �ŦX������
		from ���� as comp
			left join �ɨƥN�� as r on comp.�W�� = r.�ɨƦW��
			left join �D��� as h on comp.�W�� = h.�ɨƦW��
		where substring(comp.����, 1, charindex(',', comp.����) - 1) in ('Kaohsiung', 'Tainan')
			and (r.WCA�N�� like '%[���g�h�B�Ͷv]%'�@
			or h.�D��ζ� like '%�L�ɼ�%')) as chosen
		join ���Z as r on chosen.�ŦX������ = r.�ɨƦW��
		join �ɨƳW�w as cr on chosen.�ŦX������ = cr.�ɨƦW�� and r.���� = cr.����
		join ���� as com on chosen.�ŦX������ = com.�W��
	where r.���� = '222'
	order by isnull(r.���, cr.�ɶ�����) asc
) as union���ઽ����1
union
select *
from (
	select top 5 with ties ���, year(com.�}�l���) as �~��, chosen.�ŦX������ as ���ɦW��
		, substring(com.����, 1, charindex(',', com.����) - 1) as �Ҧb����
		, r.�ϥΤ��, 'slow' as �Ʀ�
	from (select distinct comp.�W�� as �ŦX������
		from ���� as comp
			left join �ɨƥN�� as r on comp.�W�� = r.�ɨƦW��
			left join �D��� as h on comp.�W�� = h.�ɨƦW��
		where substring(comp.����, 1, charindex(',', comp.����) - 1) in ('Kaohsiung', 'Tainan')
			and (r.WCA�N�� like '%[���g�h�B�Ͷv]%'�@
			or h.�D��ζ� like '%�L�ɼ�%')) as chosen
		join ���Z as r on chosen.�ŦX������ = r.�ɨƦW��
		join �ɨƳW�w as cr on chosen.�ŦX������ = cr.�ɨƦW�� and r.���� = cr.����
		join ���� as com on chosen.�ŦX������ = com.�W��
	where r.���� = '222'
	order by isnull(r.���, cr.�ɶ�����) desc
) as union���ઽ����2
order by �~�� desc, ��� asc;
go











select ���
from ���Z
order by ��� asc



select top 2 with ties ���u�s��, �m�W, �X�ͤ��, datediff(year, e.�X�ͤ��, getdate()) as �~��
from ���u�@as e
order by �~�� desc



select *
from (select top 2 with ties ���u�s��, �m�W, �X�ͤ��, datediff(year, e.�X�ͤ��, getdate()) as �~��
from ���u�@as e
order by �~�� desc) as a
union
select *
from (select top 2 with ties ���u�s��, �m�W, �X�ͤ��, datediff(year, e.�X�ͤ��, getdate()) as �~��
from ���u�@as e
order by �~�� asc) as b
go


















