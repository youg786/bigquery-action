SELECT 
oggi.id_archipelia as OLD_ID_Archipelia,
oggi.date as OLD_Date,
Trunc(oggi.pmp,2) as OLD_PMP,
ieri.id_archipelia as NEW_ID_Archipelia,
ieri.date as NEW_Date,
trunc(ieri.pmp,2) as NEW_PMP,
trunc(oggi.pmp - ieri.pmp,2) as DIFF_VAL, 
abs(trunc(((oggi.pmp - ieri.pmp)/oggi.pmp)*100,2)) as DIFF_PERC

--test commentaire update ok2
--test commentaire update ok3

FROM `terra-incognita-cote-ivoire.shipment.PMP_HISTORY_COMPANY` as oggi
left join `terra-incognita-cote-ivoire.shipment.PMP_HISTORY_COMPANY` as ieri
on oggi.id_archipelia = ieri.id_archipelia and cast(oggi.date as date) = date_sub(cast(ieri.date as date), interval 1 day)
order by DIFF_VAL desc

--test import nouvelle vue dans un dataset (sandbox) existant.