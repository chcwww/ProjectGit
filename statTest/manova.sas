%let indir=C:\Users\chcww\Downloads;

proc import  datafile = "&indir\take.csv" out = take
dbms = csv replace;
getnames = yes;
run;

/* MANOVA */

PROC GLM data = take;
CLASS repeater;
MODEL buy_total_amount buy_total_freq buy_total_daydiff = repeater;
MANOVA H = repeater/PRINTE ;
RUN; 

/* FACTOR */

proc factor data = hw6 res score
method = principal nfact = 2 rotate = varimax out = out1;
var x1-x7;
proc print data = out1;
run; 
