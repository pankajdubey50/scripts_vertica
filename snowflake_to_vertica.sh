#!/bin/sh

set -e
set -o pipefail
DIRNAME=$(dirname "$(readlink -f "$0")")
cd $DIRNAME
confFile="/home/produser/sigmoid/list-of-tables"
function error_reporting(){
 echo "Error generated at line no. :- $1"
}
trap 'error_reporting $LINENO' ERR
snow=$(cat ../$(echo am9obnNub3cK | base64 -d))

inputs(){
    read -p "Enter load type [ live/reprocess ]:- " loadType #required to update load state
    lowerLoadType=`echo $loadType | tr '[:upper:]' '[:lower:]'`
    if [ $lowerLoadType != "live" ] && [ $lowerLoadType != "reprocess" ]; then
        echo "loadType input is wrong. It should be [ live/reprocess ]"
        exit 0
    fi
    read -p "Enter run type [ daily/hourly/dim ]:- " runType
    lowerRunType=`echo $runType | tr '[:upper:]' '[:lower:]'`
    if [ $lowerRunType != "daily" ] && [ $lowerRunType != "hourly" ] && [ $lowerRunType != "dim" ]; then
        echo "runType input is wrong. It should be [ daily/hourly/dim ]"
        exit 0
    fi
    read -p "Want to process for all $lowerRunType tables [ Y/N ]:-" forAll
   	runForAllTables=`echo $forAll | tr '[:upper:]' '[:lower:]'`

    if [[ "$runForAllTables" == "n" ]]; then
    	 read -p "Enter Table name(for multiple tables,add space seperated table name) :- " tableList
    	 tableList=$(echo $tableList | sed 's/^ //g' | sed 's/ $//g')
    elif [[ "$runForAllTables" == "y" ]]; then
    	tableList=$(cat $confFile | grep -v "#" | grep -w "$lowerRunType" | grep -vw "no" | awk -F, '{print $1}' | xargs || true)
    else
    	echo "Invalid response."
    	exit 0
    fi
    if [[ $runType != "dim" ]];then
        read -p "Enter date [ format YYYY-MM-DD ]:- " processDate
        processDateCount=`echo -n $processDate | wc -c`
        if [ $processDateCount != 10 ]; then
            echo "You have enter wrong date. Date format should be [ YYYY-MM-DD ]"
            exit 0
        fi
        if [[ $runType == "hourly" ]];then
            read -p "Enter hour(For multiple hours,add space seperated hours.For all hours of day give {allhour}.) [ HH ]:- " hourList
            hourList=$(echo $hourList | sed 's/^ //g' | sed 's/ $//g')
            
            echo "---------------------------------------------------------------------------------------------------------------------------------------------------"
            echo "Running the script for loadType:[$lowerLoadType] runType:[$lowerRunType] TableName:[$tableList] processDate:[$processDate] ProcessHour:[$hourList]"
            echo "---------------------------------------------------------------------------------------------------------------------------------------------------"
        fi
        echo "-------------------------------------------------------------------------------------------------------------------------------------------------------"
        echo "Running the script for loadType:[$lowerLoadType] runType:[$lowerRunType] TableName:-[$tableList] processDate:[$processDate]"
        echo "-------------------------------------------------------------------------------------------------------------------------------------------------------"
    else
        echo "-------------------------------------------------------------------------------------------------------------------------------------------------------"
        echo "Running the script for loadType:[$lowerLoadType] runType:[$lowerRunType] TableName:-[$tableList]"
        echo "-------------------------------------------------------------------------------------------------------------------------------------------------------"
    fi

}

inputs

vertica_base_path="/var/mnt/mfs/snowflake_to_vertica"
schema_name=mstr_datamart
export SNOWSQL_PWD="$snow"
export SNOWSQL_WAREHOUSE='PROD_ETL_WH'
export SNOWSQL_ROLE='SYSADMIN'

for TableName in $(echo $tableList)
do
	lowerTableName=`echo $TableName | sed 's/ //g' | tr '[:upper:]' '[:lower:]'`
	validTable=`cat $confFile | grep -v "#" | grep -w "$lowerTableName" || true`
	time_column_type=`cat $confFile | grep -v "#" | grep -w "$lowerTableName" | awk -F',' '{print $5}' || true`
	if [ -z $validTable ]; then
    	echo "TableName input is wrong. Please provide correct name."
    	exit 0
	fi
	echo "Runing for Table $lowerTableName..."
	if [[ "$lowerRunType" != "hourly" ]]; then
			hourList="00"
	fi
    if [[ $hourList == "allhour" ]]; then
        hourList="00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23"
    fi
	for processHour in $(echo $hourList)
	do
		processHourCount=`echo -n $processHour | wc -c`
	    if [ $processHourCount != 2 ]; then
	        echo "You have enter wrong hour. Hour format should be [ HH ]"
	        exit 0
	    fi
	    echo "Runing for Hour $processHour..."
		LOG_PATH=/home/produser/sigmoid/tmp/$lowerTableName/
		ERR_PATH=/home/produser/sigmoid/tmp/$lowerTableName/
		if [ ! -d "$LOG_PATH" ]; then
		   mkdir -p $LOG_PATH
		   LOG="$LOG_PATH/log"
		fi
		if [ ! -d "$ERR_PATH" ]; then
		   mkdir -p $ERR_PATH
		   ERR="$ERR_PATH/err"
		fi
		SNOWSQL_OPTIONS=(
		  "-o" "log_level=DEBUG"
	          "-o" "echo=ON"
        	  "-o" "exit_on_error=True"
       	       	  "-o" "friendly=False"
	          "-o" "quiet=False"
	          "-o" "timing=False"
	          "-o" "output_file=$LOG"
	          "-o" "variable_substitution=True"
	          "-a" "openx.us-east-1"
	          "-u" "roushan.kumar@openx.com"
	          "-d" "prod"
	          "-s" "MSTR_DATAMART"
        	  "-w" "PROD_ETL_WH"
	        )
		. /home/produser/sigmoid/load_state.sh "$lowerTableName"

		vertica_year=$(echo $processDate | awk -F'-' '{print $1}')
		vertica_month=$(echo $processDate | awk -F'-' '{print $2}')
		vertica_date=$(echo $processDate | awk -F'-' '{print $3}')
		if [[ "$lowerRunType" == "daily" ]];then
			local_path="$vertica_base_path/$lowerTableName/$vertica_year/$vertica_month/$vertica_date"
			required_timestamp_format="$processDate"
			 if [[ "$time_column_type" == "sid" ]];then
                	        required_timestamp_format=$(echo $required_timestamp_format | sed 's/-//g')
                	fi
		elif [[ "$lowerRunType" == "hourly" ]]; then
			vertica_hour=$processHour
			local_path="$vertica_base_path/$lowerTableName/$vertica_year/$vertica_month/$vertica_date/$vertica_hour"
			required_timestamp_format="$processDate $processHour:00:00"
        elif [[ "$lowerRunType" == "dim" ]]; then
			vertica_hour=$processHour
			local_path="$vertica_base_path/$lowerTableName/$vertica_year/$vertica_month/$vertica_date/$vertica_hour"
			required_timestamp_format="$processDate $processHour:00:00"
		fi
		#Making sure if the vertica target is present
		if [ ! -d "$local_path" ]; then
			mkdir -p $local_path
		else 
			rm -rf $local_path
			mkdir -p $local_path
		fi

		if [[ ! -z $column_list ]]; then
		        snowflake_query="select $column_list from $schema_name.$lowerTableName where $time_column_name='$required_timestamp_format'"
        elif [[ "$lowerRunType" == "dim" ]]; then
                snowflake_query="select * from $schema_name.$lowerTableName"
        else 
		        snowflake_query="select * from $schema_name.$lowerTableName where $time_column_name='$required_timestamp_format'"
		fi
		flag="pipe"
		snowflake_to_local(){
			query_time=`date "+%s%N"`
			echo "create or replace stage prod.mstr_datamart.${lowerTableName}_sigmoid_${query_time};" > $PWD/queries/${lowerTableName}_${query_time}.sql
			if [[ $(cat $confFile | grep -v "#" | grep -w "$lowerTableName" | awk -F, '{print $3}' || true) == "pipe" ]]; then
				echo "copy into @prod.mstr_datamart.${lowerTableName}_sigmoid_${query_time} from ( $snowflake_query ) file_format = (type ='CSV' compression='GZIP' RECORD_DELIMITER='\n' FIELD_DELIMITER='0x7C' null_if = ('NULL', 'null', '\\N', '\N') empty_field_as_null=false) header = TRUE;" >> $PWD/queries/${lowerTableName}_${query_time}.sql
			elif [[ $(cat $confFile | grep -v "#" | grep -w "$lowerTableName" | awk -F, '{print $3}' || true) == "single_quote" ]]; then
				flag="single_quote"
				echo "copy into @prod.mstr_datamart.${lowerTableName}_sigmoid_${query_time} from ( $snowflake_query ) file_format = (type ='CSV' compression='GZIP' RECORD_DELIMITER='\n' FIELD_DELIMITER='\x1E' FIELD_OPTIONALLY_ENCLOSED_BY='\'' null_if = ('NULL', 'null', '\\N', '\N') empty_field_as_null=false) header = TRUE;" >> $PWD/queries/${lowerTableName}_${query_time}.sql
			elif [[ $(cat $confFile | grep -v "#" | grep -w "$lowerTableName" | awk -F, '{print $3}' || true) == "double_quote" ]]; then
				flag="double_quote"
				echo "copy into @prod.mstr_datamart.${lowerTableName}_sigmoid_${query_time} from ( $snowflake_query ) file_format = (type ='CSV' compression='GZIP' RECORD_DELIMITER='\n' FIELD_DELIMITER='\x1E' FIELD_OPTIONALLY_ENCLOSED_BY='\"' null_if = ('NULL', 'null', '\\N', '\N') empty_field_as_null=false) header = TRUE;" >> $PWD/queries/${lowerTableName}_${query_time}.sql
			else
				echo "Invalid delimiter flag type."
				exit 0
			fi
			echo "get @prod.mstr_datamart.${lowerTableName}_sigmoid_${query_time} file://$local_path;" >> $PWD/queries/${lowerTableName}_${query_time}.sql
			snowsql ${SNOWSQL_OPTIONS[@]} -f $PWD/queries/${lowerTableName}_${query_time}.sql
			snowsql ${SNOWSQL_OPTIONS[@]} -q "drop stage prod.mstr_datamart.${lowerTableName}_sigmoid_${query_time};"
		}

		push_to_vertica(){
			if [[ "$lowerRunType" != "dim" ]] && [[ "$lowerTableName" != "supply_demand_country_hourly_fact" ]]; then
			#if [[ "$lowerRunType" != "dim" ]]; then
				echo "Deleting data from vertica before pushing it"
				vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "delete from MSTR_DATAMART.$lowerTableName where $time_column_name='$required_timestamp_format';commit;"
	                elif [[ "$lowerRunType" == "dim" ]]; then
				echo "Dropping table $lowerTableName from vertica before pushing it"
				#vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "drop table MSTR_DATAMART.$lowerTableName;commit;"
                		vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "set search_path=mstr_datamart; drop table if exists mstr_datamart.${lowerTableName}_temp2;create table mstr_datamart.${lowerTableName}_temp2 like mstr_datamart.${lowerTableName};copy mstr_datamart.${lowerTableName}_temp2 from '/var/mnt/mfs/snowflake_to_vertica/${lowerTableName}/00/*' on any node GZIP DELIMITER E'\036' ENCLOSED BY E'\"' NULL 'NULL' REJECTED DATA AS TABLE mstr_datamart.${lowerTableName}_rej;begin;alter table ${lowerTableName}, ${lowerTableName}_temp2, temp rename to temp, ${lowerTableName}, ${lowerTableName}_temp2;Grant Select on all tables in Schema mstr_datamart to vertica_mstr_er;Grant Select on all tables in Schema mstr_datamart to vertica_mstr_er_cube;Grant Select on all tables in Schema mstr_datamart to public;commit;drop table if exists mstr_datamart.${lowerTableName}_temp2;"
                		echo "Done with loading dim table $lowerTableName"
			fi
			echo "Pushing data in vertica for $lowerTableName"
			
			#	count_before_insert=`vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select count(*) from MSTR_DATAMART.$lowerTableName where $time_column_name='$required_timestamp_format'" | tail -1`

			if [[ "$flag" == "pipe" ]] && [[ "$lowerRunType" != "dim" ]]; then
				count_before_insert=`vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select count(*) from MSTR_DATAMART.$lowerTableName where $time_column_name='$required_timestamp_format'" | tail -1`
				vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "copy MSTR_DATAMART.$lowerTableName from '$local_path/*' on any node GZIP DELIMITER E'|' NULL 'NULL'  REJECTED DATA AS TABLE test.$lowerTableName;commit;"
				count_after_insert=`vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select count(*) from MSTR_DATAMART.$lowerTableName where $time_column_name='$required_timestamp_format'"`
			elif [[ "$flag" == "single_quote" ]] && [[ "$lowerRunType" != "dim" ]]; then
				count_before_insert=`vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select count(*) from MSTR_DATAMART.$lowerTableName where $time_column_name='$required_timestamp_format'" | tail -1`
				vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "copy MSTR_DATAMART.$lowerTableName from '$local_path/*' on any node GZIP DELIMITER E'\036' ENCLOSED BY E'\'' NULL 'NULL'  REJECTED DATA AS TABLE test.$lowerTableName;commit;"
				count_after_insert=`vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select count(*) from MSTR_DATAMART.$lowerTableName where $time_column_name='$required_timestamp_format'"`
			elif [[ "$flag" == "double_quote" ]] && [[ "$lowerRunType" != "dim" ]]; then
				count_before_insert=`vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select count(*) from MSTR_DATAMART.$lowerTableName where $time_column_name='$required_timestamp_format'" | tail -1`
				vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "copy MSTR_DATAMART.$lowerTableName from '$local_path/*' on any node GZIP DELIMITER E'\036' ENCLOSED BY E'\"' NULL 'NULL'  REJECTED DATA AS TABLE test.$lowerTableName;commit;"
				count_after_insert=`vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select count(*) from MSTR_DATAMART.$lowerTableName where $time_column_name='$required_timestamp_format'"`
			else
				echo "Invalid delimiter flag type."
				#exit 0
			fi
	 	sleep 2
#		count_after_insert=`vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select count(*) from MSTR_DATAMART.$lowerTableName where $time_column_name='$required_timestamp_format'"`
		
#		while [ $count_after_insert -le 0 ]
#                  do
#   		     echo "Waiting for insert operation to complete"
#		sleep 5
#		     count_after_insert=`vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select count(*) from MSTR_DATAMART.$lowerTableName where $time_column_name='$required_timestamp_format'" | tail -1`
#		done


			#echo "##################################################################################################################"
			#echo "COUNT BEFORE ($lowerTableName): $count_before_insert"
			#echo "COUNT AFTER ($lowerTableName): $count_after_insert"
			#echo "###################################################################################################################"
		
		}
		if [ $? -ne 0 ]
		then
			echo "Error while processing inputs.Please check."
			exit 0
		fi
		snowflake_to_local
		if [ $? -ne 0 ]
		then
			echo "Error while unloading data from snoflake."
			exit 0
		fi
		push_to_vertica
		if [ $? -ne 0 ]
		then
			echo "Error while uploading data into vertica."
			exit 0
		fi
		if [[ "$lowerLoadType" == "live" ]] && [[ "$lowerRunType" == "hourly" ]] && [[ "$lowerTableName" != "supply_demand_country_hourly_fact" ]]; then
                        echo "Checking if eligible to update load state"
                        current_time=$(vsql -t -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select variable_value from mstr_datamart.load_state where variable_name='$load_state_variable_name'" | tail -2 | sed 's/^ //g')
                        if [[ "$current_time" < "$required_timestamp_format" ]];then
                                vsql -t -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "update mstr_datamart.load_state set variable_value = '$required_timestamp_format' where variable_name = '$load_state_variable_name';commit;"

echo "***********************UPDATING HISTORY TABLE***************************"

                                vsql -t -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "insert into mstr_datamart.load_state_history (variable_name, variable_value, modified_datetime) values ('$load_state_variable_name','$required_timestamp_format',current_timestamp(2));commit;"

                                if [ $? -eq 0 ]; then
                                        echo "Vertica Loadstate update query ran successfully "
                                else
                                        echo "Error while updating load state"
                                        exit 1
                                fi
                                echo "Load state updated to $required_timestamp_format for $lowerTableName"
                        else
                                echo "Not eligible hour to update Load state"
                        fi
                elif [[ "$lowerRunType" == "daily" ]]; then
                        current_time=$(vsql -t -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select variable_value from mstr_datamart.load_state where variable_name='$load_state_variable_name'" | tail -2 | sed 's/^ //g')
                        if [[ "$current_time" < "$required_timestamp_format 00:00:00" ]];then
                                snwflkTime=`snowsql ${SNOWSQL_OPTIONS[@]} -q "select variable_value from mstr_datamart.load_state where variable_name='$load_state_variable_name'" | tail -2 | awk -F'|' '{print $2}'| sed 's/^ //g' | sed 's/ $//g'`
				echo "====== Snowflake Time-> $snwflkTime"
                                vsql -t -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "update mstr_datamart.load_state set variable_value = '$required_timestamp_format' where variable_name = '$snwflkTime';commit;"
				
echo "***********************UPDATING HISTORY TABLE***************************"
		vsql -t -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "insert into mstr_datamart.load_state_history (variable_name, variable_value, modified_datetime) values ('$load_state_variable_name','$required_timestamp_format',current_timestamp(2));commit;"
                                if [ $? -eq 0 ]; then
                                        echo "Vertica Loadstate update query ran successfully "
                                else
                                        echo "Error while updating load state"
                                        exit 1
                                fi
                                echo "Load state updated to $snwflkTime for $lowerTableName"
                        else
                                echo "Not eligible hour to update Load state"
                        fi
                else
                        echo "no need to update load state"
                fi
	
echo "Running Update on Inventory Dashboard-----------------------------------------------"
		
	if [[ "$lowerTableName" == "supply_demand_country_hourly_fact" ]]; then

        #vsql -t -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "insert into mstr_dashboarddb.order_fact ( utc_timestamp,utc_date_sid,platform_id,advertiser_account_nk,order_nk,a_currency_code,tot_impressions,tot_billable_impressions,tot_clicks,tot_conversions,tot_spend,tot_conversion_spend,is_neartime) select utc_timestamp, utc_date_sid,REPLACE(p_platform_id,'=',''),advertiser_account_nk,order_nk,a_currency_code,sum(tot_impressions),sum(tot_billable_impressions),sum(tot_clicks),sum(tot_click_conversions),sum(tot_spend),sum(tot_conversion_spend), false from mstr_datamart.supply_demand_country_hourly_fact where utc_timestamp = '$required_timestamp_format' group by 1,2,3,4,5,6;commit;"

	sleep 2s

        #vsql -t -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "insert into mstr_dashboarddb.publisher_account_fact ( utc_timestamp,utc_date_sid,platform_id,publisher_account_nk,p_currency_code,tot_requests,tot_impressions,tot_billable_impressions,tot_clicks,tot_conversions,tot_publisher_revenue,tot_publisher_conversion_revenue,tot_network_revenue,tot_network_conversion_revenue,is_neartime) select utc_timestamp, utc_date_sid,p_platform_id,publisher_account_nk,p_currency_code,sum(tot_requests),sum(tot_impressions),sum(tot_billable_impressions),sum(tot_clicks),sum(tot_click_conversions),sum(tot_publisher_revenue),sum(tot_publisher_conversion_revenue),sum(tot_network_revenue),sum(tot_network_conversion_revenue), false from mstr_datamart.supply_demand_country_hourly_fact where utc_timestamp = '$required_timestamp_format' group by 1,2,3,4,5;commit;"

	sleep 2s

        #vsql -t -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "insert into mstr_dashboarddb.site_fact ( utc_timestamp, utc_date_sid, platform_id, publisher_account_nk, site_nk, p_currency_code, tot_requests, tot_impressions, tot_billable_impressions, tot_clicks, tot_conversions, tot_publisher_revenue, tot_publisher_conversion_revenue, tot_network_revenue, tot_network_conversion_revenue, is_neartime) select utc_timestamp, utc_date_sid , p_platform_id, publisher_account_nk, site_nk, p_currency_code, sum(tot_requests), sum(tot_impressions), sum(tot_billable_impressions), sum(tot_clicks), sum(tot_click_conversions), sum(tot_publisher_revenue), sum(tot_publisher_conversion_revenue), sum(tot_network_revenue), sum(tot_network_conversion_revenue), false from mstr_datamart.supply_demand_country_hourly_fact where utc_timestamp = '$required_timestamp_format' group by 1,2,3,4,5,6;commit;"

	else
        	echo "No need to Update on Inventory Dashboard for $lowerTableName"
		
	fi

#echo "---------------Running ANALYZE_STATISTICS on All Tables----------------------------------"
	#vsql -h vertica-edwxv.xv.dc.openx.org -U vertica_etl -w "DBApv3t1ca" DW -c "select ANALYZE_STATISTICS('$lowerTableName');"

	done

done
echo "---------------------------------------------------------------------------------------------------------------------------------------------------"
echo "Done for loadType:[$lowerLoadType] runType:[$lowerRunType] TableName:[$tableList] processDate:[$processDate] ProcessHour:[$hourList]"
echo "---------------------------------------------------------------------------------------------------------------------------------------------------"

