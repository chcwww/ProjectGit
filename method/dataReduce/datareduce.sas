proc import datafile = "C:\Users\User\Downloads\transactions.csv"
out = tran dbms = csv;
getnames = yes;
run;

proc print data = tran(obs = 10);
run;

proc freq data = tran;
table category;
run;

proc import datafile = "C:\Users\User\Downloads\offers.csv"
out = offer dbms = csv;
getnames = yes;
run;

proc freq data = offer;
table category;
run;

data trann;
set tran;
if category ne "706" and category ne "799" and  category ne "1703" and category ne "1726" and
category ne "2119" and category ne "2202"  and category ne "3203" and category ne "3504" and
category ne "3509" and category ne "4401"  and category ne "4517" and category ne "5122" and
category ne "5558" and category ne "5616"  and category ne "5619" and category ne "5824" and
category ne "6202" and category ne "7205" and category ne "9115" and category ne "9909" then delete;
run; 

proc freq data = trann;
table category;
run;

proc export data = trann outfile = "C:\Users\User\Downloads\newdata.csv"
dbms = csv replace;
run;

