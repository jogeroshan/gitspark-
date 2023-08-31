###Load Ref Tables###
hive -e "drop database prod cascade;"

hive -e "create database prod;"

hostname="jdbc:postgresql://postgre.cxy82sboupot.ap-south-1.rds.amazonaws.com:5432/prod?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"
user="puser"
pwd="ppassword"


#ADDRESS
#CITY
#COUNTRY
#DEPARTMENT
#DISEASE
#DOCTOR
#E_ADDRESS
#HOSPITAL
#POLICY
#STATE
#TEST

###ADDRESS

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table address --create-hive-table --hive-table prod.address_hve --hive-import

echo "Address_hve Table Imported"

###CITY


sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table city --create-hive-table --hive-table prod.city_hve --hive-import

echo "city_hve Table Imported"

###COUNTRY


sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table country --create-hive-table --hive-table prod.country_hve --hive-import

echo "country_hve Table Imported"

###DEPARTMENT


sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table department  --create-hive-table --hive-table prod.department_hve --hive-import

echo "department_hve Table Imported"
 
###DISEASE


sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table disease --create-hive-table --hive-table prod.disease_hve --hive-import

echo "disease_hve Table Imported"

###DOCTOR


sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table doctor --create-hive-table --hive-table prod.doctor_hve --hive-import

echo "doctor_hve Table Imported"

###E_ADDRESS 


sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table e_address --create-hive-table --hive-table prod.e_address_hve --hive-import

echo "e_address_hve Table Imported"

###HOSPITAL

sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table hospital --create-hive-table --hive-table prod.hospital_hve --hive-import

echo "hospital_hve Table Imported"

###POLICY


sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table policy --create-hive-table --hive-table prod.policy_hve --hive-import

echo "policy_hve Table Imported"

###STATE


sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table state --create-hive-table --hive-table prod.state_hve --hive-import

echo "state_hve Table Imported"

###TEST


sqoop import --connect $hostname --username $user --password $pwd -m 1 --fetch-size 10 --table test --create-hive-table --hive-table prod.test_hve --hive-import

echo "test_hve Table Imported" 

echo "#######ALL REF TABLES LOADED#########"  