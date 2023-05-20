use CH07範例資料庫

/*05【難易度 : ★★★☆☆】10%
請列出每一年的產品暢銷排行榜
[輸出](*年度, 類別名稱, 產品名稱, *銷售數量, *銷售排名, *備註說明)
[說明]
銷售排名 : 依據銷售數量，最高者為第1名 (使用 dense_rank())
備註 請依據名次來給定 備註說明
銷售排名 1-3 名 : 極暢銷
銷售排名 4-6 名 : 暢銷
其他            : 空白
*/
--【Solution】
select year(o.訂貨日期) as 年度, ca.類別名稱, p.產品名稱
	 , sum(od.數量) as 銷售數量
	 , dense_rank() over(partition by year(o.訂貨日期) order by sum(od.數量) desc) as 秒數排名
	 , case 
	   when dense_rank() over(partition by year(o.訂貨日期) order by sum(od.數量) desc) in (1, 2, 3) then '狀態超常'
	   when dense_rank() over(partition by year(o.訂貨日期) order by sum(od.數量) desc) in (4, 5, 6) then '狀態良好'
	   else '' end as 備註說明
from 訂單 as o, 訂單明細 as od, 產品資料 as p, 產品類別 as ca
where o.訂單編號 = od.訂單編號
  and od.產品編號 = p.產品編號
  and p.類別編號 = ca.類別編號
group by year(o.訂貨日期), ca.類別名稱, p.產品名稱
go


use 資料庫系統期末報告;

select 賽事名稱, 年度, 秒數, 名稱, 秒數排名, 狀態
from (select r.賽事名稱, year(com.開始日期) as 年度, r.秒數, c.名稱
	 , dense_rank() over(partition by year(com.開始日期) order by isnull(r.秒數, cr.時間限制) asc) as 秒數排名
	 , case 
	   when dense_rank() over(partition by year(com.開始日期) order by isnull(r.秒數, cr.時間限制) asc) in (1, 2, 3) then '良好'
	   when dense_rank() over(partition by year(com.開始日期) order by isnull(r.秒數, cr.時間限制) asc) in (4, 5, 6) then '一般'
	   else '不理想'
	   end as 狀態
from 成績 as r, 方塊 as c, 比賽 as com, 賽事規定 as cr
where r.使用方塊 = c.名稱
	and r.項目 = '333'
	and r.賽事名稱 = com.名稱
	and com.名稱 = cr.賽事名稱
	and cr.項目 = '333'
group by year(com.開始日期), r.賽事名稱, r.秒數, c.名稱, cr.時間限制) as tableA
where 秒數排名 <= 8;
go




go


select *
from 成績 as r, 比賽 as com, 賽事規定 as cr
where r.項目 = '333'
	and r.賽事名稱 = com.名稱
	and com.名稱 = cr.賽事名稱











update 方塊
set 配色 = '黑底'
where 配色 = '白底'
go

select *
from 方塊







-- substring(c.城市, 1, charindex(',', c.城市) - 1)


select c.名稱, count(*)
from 比賽 as c
	join 攜帶方塊 as b on c.名稱 = b.賽事名稱
group by c.名稱



select k.年度, k.賽事名稱, convert(numeric(5, 3)
	, t.攜帶方塊數 / (k.攜帶方塊數*1.0)) as 攜帶方塊使用比率
	, k.縣市
from (select 賽事名稱, count(使用方塊) as 攜帶方塊數
	from (select 賽事名稱, 使用方塊
		from 成績
		group by 賽事名稱, 使用方塊) as tableB
		group by 賽事名稱) as t
	join (select comp.名稱 as 賽事名稱
		, count(r.使用方塊) as 攜帶方塊數
		, substring(comp.城市, 1, charindex(',', comp.城市) - 1) as 縣市
		, year(comp.開始日期) as 年度
		from 比賽 as comp 
			join 成績 as r on r.賽事名稱 = comp.名稱
		group by comp.名稱
			, substring(comp.城市, 1, charindex(',', comp.城市) - 1)
			, year(comp.開始日期)) as k on t.賽事名稱 = k.賽事名稱
order by 年度 desc, 攜帶方塊使用比率 desc;
go



select distinct comp.名稱 as 符合的比賽
from 比賽 as comp
	left join 賽事代表 as r on comp.名稱 = r.賽事名稱
	left join 主辦方 as h on comp.名稱 = h.賽事名稱
where substring(comp.城市, 1, charindex(',', comp.城市) - 1) in ('Kaohsiung', 'Tainan')
	and (r.WCA代表 like '%[郭君逸劉睿鈞]%'　
	or h.主辦團隊 like '%林珈樂%')
	

select *
from (
	select top 5 with ties 秒數, year(com.開始日期) as 年度, chosen.符合的比賽 as 比賽名稱
		, substring(com.城市, 1, charindex(',', com.城市) - 1) as 所在縣市
		, r.使用方塊, 'fast' as 排位
	from (select distinct comp.名稱 as 符合的比賽
		from 比賽 as comp
			left join 賽事代表 as r on comp.名稱 = r.賽事名稱
			left join 主辦方 as h on comp.名稱 = h.賽事名稱
		where substring(comp.城市, 1, charindex(',', comp.城市) - 1) in ('Kaohsiung', 'Tainan')
			and (r.WCA代表 like '%[郭君逸劉睿鈞]%'　
			or h.主辦團隊 like '%林珈樂%')) as chosen
		join 成績 as r on chosen.符合的比賽 = r.賽事名稱
		join 賽事規定 as cr on chosen.符合的比賽 = cr.賽事名稱 and r.項目 = cr.項目
		join 比賽 as com on chosen.符合的比賽 = com.名稱
	where r.項目 = '222'
	order by isnull(r.秒數, cr.時間限制) asc
) as union不能直接併1
union
select *
from (
	select top 5 with ties 秒數, year(com.開始日期) as 年度, chosen.符合的比賽 as 比賽名稱
		, substring(com.城市, 1, charindex(',', com.城市) - 1) as 所在縣市
		, r.使用方塊, 'slow' as 排位
	from (select distinct comp.名稱 as 符合的比賽
		from 比賽 as comp
			left join 賽事代表 as r on comp.名稱 = r.賽事名稱
			left join 主辦方 as h on comp.名稱 = h.賽事名稱
		where substring(comp.城市, 1, charindex(',', comp.城市) - 1) in ('Kaohsiung', 'Tainan')
			and (r.WCA代表 like '%[郭君逸劉睿鈞]%'　
			or h.主辦團隊 like '%林珈樂%')) as chosen
		join 成績 as r on chosen.符合的比賽 = r.賽事名稱
		join 賽事規定 as cr on chosen.符合的比賽 = cr.賽事名稱 and r.項目 = cr.項目
		join 比賽 as com on chosen.符合的比賽 = com.名稱
	where r.項目 = '222'
	order by isnull(r.秒數, cr.時間限制) desc
) as union不能直接併2
order by 年度 desc, 秒數 asc;
go











select 秒數
from 成績
order by 秒數 asc



select top 2 with ties 員工編號, 姓名, 出生日期, datediff(year, e.出生日期, getdate()) as 年齡
from 員工　as e
order by 年齡 desc



select *
from (select top 2 with ties 員工編號, 姓名, 出生日期, datediff(year, e.出生日期, getdate()) as 年齡
from 員工　as e
order by 年齡 desc) as a
union
select *
from (select top 2 with ties 員工編號, 姓名, 出生日期, datediff(year, e.出生日期, getdate()) as 年齡
from 員工　as e
order by 年齡 asc) as b
go


















