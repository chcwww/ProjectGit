%let indir=C:\Users\chcww\Downloads;

proc import  datafile = "&indir\take.csv" out = take
dbms = csv replace;
getnames = yes;
run;




PROC GLM data = take;
CLASS repeater;
MODEL buy_total_amount buy_total_freq buy_total_daydiff = repeater;
MANOVA H = repeater/PRINTE ;
RUN; 
