*****************************************************************************
*            STUDY : BergenBio BGBC003 AML
*          PROGRAM : Group_study.sas 
*           SAS VS : 9.4
*           AUTHOR : Group Project
*         CREATED  : 
*         COMMENTS :
*  Macro Parameters:  
*****************************************************************************;
/*/ define options /*/
options nofmterr mautosource append=sasautos=(<filenames>)  nodate nonumber missing=' ' yearcutoff=1930 mrecall 
fmtsearch=(work) nocenter mergenoby=warn dkricond=warn dkrocond=warn msglevel=i ps=60 ls=120 
formchar="|____|+|___+=|_/\<>*" source source2 mprint mlogic orientation=landscape validvarname=upcase;;
options fmtsearch=(projects.format);
 proc datasets lib=work kill;run;
/*/ Create lnrlabid for all observations /*/
proc sort data=project.hema(where=(HEMAND NE 'ND' )) out=hem01;
    by scrnid evtorder evtnam row;
run;
proc sort data=project.INVALID(WHERE=(frmnam='HEMA')) out=invalhm;
    by scrnid evtorder evtnam row;
run;
******************************************
**Merge Hema and Invalid
******************************************;
Data hema1;
length LBORRES $20;
    merge hem01
                invalhm(drop= STUDYNAM PROTLBL SITEKEY SITEID SITENAM PROTRGKY PATKY 
                PATID ENRLID PATINIT PATSTA PATEVTKY REGEVTKY EVTLBL EVTFRMKY GRPNAM PatFrmKy);
    by scrnid evtorder evtnam row;
    
      if HemaOrr NE . then LBORRES = Hemaorr;
        else if HemaOrr EQ . then LBORRES=chk;
        
        drop chk;
run;
proc sort data=hema1;
    by scrnid evtorder evtnam evtlbl evtdt row HemaTest;
run;
/*/ seperate and merge with the same dataset/*/
data hema2(drop=grpnam compress=yes);
    merge hema1(where=(grpnam like '%S1')in=s1 keep=scrnid evtorder evtnam evtlbl evtdt grpnam hemllid hemanorm hemadt siteid LBORRES)
                hema1(where=(grpnam like '%_T1')in=t1 keep=scrnid evtorder evtnam evtlbl evtdt grpnam hematest hemaorr hemand hemclsig hemage siteid LBORRES);
    by scrnid evtorder evtnam evtlbl evtdt;
    
    if t1;
    if LBORRES ne "Not Done";
    
run;
/*/ Create LNRLABID for all observations  using LNR_H/*/
/*/ Create two datasets range_s and range_t/*/
/*/ where grpnam is '_S' and '_T'/*/
/*/ Left join the two datasets/*/
data range_s;
    set project.lnr_h;
    where index(grpnam,'_S') gt 0;
    
        keep scrnid PATEVTKY EvtOrder grpnam LBLABNAM LNRLABID;
run;
data range_t;
    set project.lnr_h;
    where index(grpnam,'_T') gt 0;
    
        drop LBLABNAM LNRLABID;
run;
    
proc SQL;
    create table range AS
        select a.*, b.LBLABNAM ,b.LNRLABID
        from range_t as a 
        left join range_s as b ON
        a.scrnid=b.scrnid and a.patevtky=b.patevtky and a.evtorder=b.evtorder;
quit;
/*/  sort range siteID lnrlabid lnrparm lbsex /*/
proc sort data=range out=lnrh_;
    by siteid lnrlabid lnrparm lbsex;
run;
/*/ change scrnid to subjid for hema dataset to merge with dm /*/
data hema3;
length subjid $40;
    set hema2;
    subjid=scrnid;
    drop scrnid;
run;
/*/ sort by subjid and siteid for hema dataset to merge with dm /*/
proc sort data=hema3;
    by subjid siteid;
run;
proc sort data=project.dm out=dm;
    by subjid siteid;
run;
/*/  hema dataset to merge with dm /*/
data hemaDm;
    merge hema3(in=a)
                dm(in=b keep= age subjid sex brthdtc armcd siteid  country where=(ARMCD ne 'SCRNFAIL'));
    by subjid siteid;
        if a;
     if hemand NE 'ND';
     
     if hemadt eq . then hemadt = evtdt;
     if evtdt eq . then evtdt = hemadt;
run;
/*/ Create Male and Female datasets to get accurate Date ranges in next datastep /*/
/*/ if min age is missing then set to 18/*/
/*/ if max age is missing then set to 99/*/
/*/ output to each male and female dataset/*/
/*/ set length of sex/*/
/*/ if sex is missing then ??????? /*/
/*/ if sex = 'N/A' then ???????/*/
data lnrh_m lnrh_f;
    length sex $3.;
    set range(drop= sitekey patid enrlid scrnid patinit patsta evtnam evtlbl evtdt evtdtyn);
    
    if lbsex = 'M' then do; 
            SEX = 'M'; output lnrh_m;
            end;
        else if lbsex = 'F' then do;
            SEX = 'F'; output lnrh_f;
            end;
        else if lbsex = 'N/A' then do;
            SEX = 'M'; output lnrh_m;
            SEX = 'F'; output lnrh_f;
            end;
        else if lbsex = '' then do;
            SEX = 'M'; output lnrh_m;
            SEX = 'F'; output lnrh_f;
            end;
run;
data male;
    set lnrh_m;           
        if missing(lbminage) then lbminage=18;
                    
    if lbminage NE . AND lbminage GE 18 then lbminage = lbminage;
    if lbminage LT 18   then lbminage = 18;
        else if lbminage EQ . then lbminage = 18;
    if lbmaxage NE . then lbmaxage = lbmaxage;
        else if lbmaxage EQ . then lbmaxage = 99;
        
    if lbmaxage ge 99 then lbmaxage=99;
run;
data female;
    set lnrh_f;           
        if missing(lbminage) then lbminage=18;
                    
    if lbminage NE . AND lbminage GE 18 then lbminage = lbminage;
    if lbminage LT 18   then lbminage = 18;
        else if lbminage EQ . then lbminage = 18;
    if lbmaxage NE . then lbmaxage = lbmaxage;
        else if lbmaxage EQ . then lbmaxage = 99;
        
    if lbmaxage ge 99 then lbmaxage=99;
run;
/*/ Sort the data to calculate date ranges /*/
proc sort data=male out=LNRH_M2 nodupkey dupout=dups;
    by SITEID LNRLABID LNRPARM LBMINAGE LBDTFR;
run;
proc sort data=female out=LNRH_F2;
    by SITEID LNRLABID LNRPARM LBMINAGE LBDTFR;
run;
    /*/ Calculate date ranges, Make unit column (including 'other' units), and add min/max ages for missing values /*/
%macro DateTo (DataIn=, DataOut=);
    data &DataOut;
        length DtFr DtTo 8.;
        obs1 = 1;
        do while (obs1 <= nobs);
            set &DataIn nobs=nobs;
            by SITEID LNRLABID LNRPARM LBMINAGE LBDTFR;
            obs2 = obs1 + 1;
            set 
                &DataIn(keep= lbdtfr rename=(LBDTFR=dt_from))
                point=obs2;
            
            if LBDTTO EQ . and last.lnrparm NE 1 and last.LBMINAGE NE 1 then DtTo = dt_from - 1;
                else if LBDTTO NE . then DtTo = LBDTTO;
                else if LBDTTO EQ . then DtTo = today();    
            if DtTo = . then DtTo=today();
            if LBDTFR NE . then DtFr = LBDTFR;
                else if LBDTFR = . then DtFr = '01JAN2000'd;
            
            if LNRUNT NE 'OTHER' then LBORRESU = upcase(LNRUNT);
                else if LNRUNT eq 'OTHER' then LBORRESU = upcase(LBLABOTH); 
            output; 
            obs1 +1;
        end;
        drop obs2 lblaboth dt_from;
        format DtFr DtTo date9.;
    run;
%mend DateTo;
%DateTo(DataIn=LNRH_M2, DataOut=LNRH_M3);
%DateTo(DataIn=LNRH_F2, DataOut=LNRH_F3);
data all_lr;
    set LNRH_M3
            LNRH_F3;
run;
/*/ Merge LabRanges to Hema Dataset /*/
proc SQL;
    create table RangeHema AS
    select h.*,lr.LBLNR as LBSTNRLO, lr.LBHNR as LBSTNRHI, lr.LBMINAGE, lr.LBMAXAGE, lr.DtFr as EffStartDt, 
                lr.DtTo as EffEndDt, lr.LBORRESU, lr.lnrlabid,h.evtlbl as Visit
        from hemaDm as h left join all_lr as lr
        on h.HemaTest = lr.LNRPARM and h.Age between lr.LBMINAGE and lr.LBMAXAGE and h.sex=lr.sex
                and h.siteid = lr.siteid and h.HemaDt between lr.DtFr and lr.DtTo and h.hemllid=lr.lnrlabid;
quit;
proc sort data=RangeHema ;
    by subjid lborresu visit siteid;
    where HEMATEST in ('HEMOGLOBIN' 'WBC' 'RBC' );
run;

data final;
	set RangeHema;
	
	if lborresu ='G/L' then lborres1=lborres*.1;
		if lborres1='' then lborres1=lborres;

	
	if lborresu= 'G/L' then lborresu1 ='G/DL';
	if lborresu1='' then lborresu1=lborresu;
	
	if sex = 'M' and HEMATEST = 'HEMOGLOBIN' and lborres1 lt 13.5 then LEVELS= 'Below Normal';
		else if sex = 'M' and HEMATEST = 'HEMOGLOBIN' and  lborres1 gt 17.5 then LEVELS='High';
		else if sex='M' and HEMATEST = 'HEMOGLOBIN' and lborres1 ge 13.5 and  lborres1 le 17.5 then  LEVELS='Normal';
	
	if sex = 'F' and HEMATEST = 'HEMOGLOBIN' and lborres1 lt 12.0 then LEVELS= 'Below Normal';
		else if sex = 'F' and HEMATEST = 'HEMOGLOBIN' and  lborres1 gt 15.5 then LEVELS='High';
		else if sex='F' and HEMATEST = 'HEMOGLOBIN' and lborres1 ge 12.0 and  lborres1 le 15.5 then  LEVELS='Normal';
	
	
	if sex = 'M' and HEMATEST = 'WBC' and lborres1 lt 5.0 then LEVELS= 'Below Normal';
		else if sex = 'M' and HEMATEST = 'WBC' and  lborres1 gt 10.0 then LEVELS='High';
		else if sex='M' and HEMATEST = 'WBC' and lborres1 ge 5.0 and  lborres1 le 10.0 then  LEVELS='Normal';
	
	if sex = 'F' and HEMATEST = 'WBC' and lborres1 lt 5.0 then LEVELS= 'Below Normal';
		else if sex = 'F' and HEMATEST = 'WBC' and  lborres1 gt 10.0 then LEVELS='High';
		else if sex='F' and HEMATEST = 'WBC' and lborres1 ge 5.0 and  lborres1 le 10.0 then  LEVELS='Normal';
		
		
	if sex = 'M' and HEMATEST = 'RBC' and lborres1 lt 4.69 then LEVELS= 'Below Normal';
		else if sex = 'M' and HEMATEST = 'RBC' and  lborres1 gt 6.13 then LEVELS='High';
		else if sex='M' and HEMATEST = 'RBC' and lborres1 ge 4.69 and  lborres1 le 6.13 then  LEVELS='Normal';
	
	if sex = 'F' and HEMATEST = 'RBC' and lborres1 lt 4.04 then LEVELS= 'Below Normal';
		else if sex = 'F' and HEMATEST = 'RBC' and  lborres1 gt 5.48 then LEVELS='High';
		else if sex='F' and HEMATEST = 'RBC' and lborres1 ge 4.04 and  lborres1 le 5.48 then  LEVELS='Normal';	
	
	if HEMATEST='WBC' and LBORRESU1 ne 'K/UL' then delete;
	if HEMATEST='RBC' and LBORRESU1 ne 'M/UL' then delete;
	if HEMATEST='HEMOGLOBIN' and LBORRESU1 ne 'G/DL' then delete;
		
	drop lborres lborresu lbstnrlo lbstnrhi lbminage lbmaxage hemage;

	
run;

proc sort data=final;
    by subjid;
run;



libname final "C:\Users\jdb3750\Desktop\SAS project"; *path th where you want the final dataset is to be stored;

data final.final; set final; run;