# %%


# %%
# 1.1  create a Python and PySpark SQL program to read/extract the following JSON files according to the specifications found in the mapping document.
import findspark
findspark.init()
from pyspark.sql import SparkSession #Importing the Libraries
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df = spark.read.option('multiline',True).load("cdw_sapp_customer.json",format='json')

# %%


# %%


# %%
# change data type
from pyspark.sql.functions import to_timestamp
df=df.withColumn('LAST_UPDATED',to_timestamp("LAST_UPDATED"))

# %%


# %%
from pyspark.sql.functions import col
df = df.withColumn('SSN', col('SSN').cast('Integer'))

# %%


# %%
# Mapping: firstname
from pyspark.sql.functions import initcap
df=df.withColumn("FIRST_NAME", initcap(col('FIRST_NAME')))

# %%
# Mapping: middlename
from pyspark.sql.functions import lower
df=df.withColumn("MIDDLE_NAME", lower(df["MIDDLE_NAME"]))

# %%
# Mapping: lastname
df=df.withColumn("LAST_NAME", initcap(col('LAST_NAME')))

# %%
# Mapping: full_street_address
from pyspark.sql.functions import concat,lit
df=df.withColumn('FULL_STREET_ADDRESS',concat(col('STREET_NAME'),lit(','), col('APT_NO')))
df=df.drop('APT_NO','STREET_NAME')

# %%


# %%
# change phone datatype and format
from pyspark.sql.functions import udf
@udf
def phone_num(phone_num):
    new_phone_num = '('+str(phone_num)[:3]+')'+str(phone_num)[3:6]+'-'+str(phone_num)[6:]
    return new_phone_num

# %%
df = df.withColumn('CUST_PHONE',phone_num('CUST_PHONE'))

# %%


# %%
# Change for cdw_sapp_branch
df2 = spark.read.option('multiline',True).load("cdw_sapp_branch.json",format='json')

# %%


# %%
# branch_zip: If the source value is null load default (99999) value else Direct move
@udf
def branch_zip(zip):
    if zip is None:
        return str(99999)
    return str(zip)

# %%
df2 = df2.withColumn('BRANCH_ZIP',branch_zip('BRANCH_ZIP'))

# %%
#BRANCH_PHONE:Change the format of phone number to (XXX)XXX-XXXX
@udf
def phone_num2(phone_num):
    new_phone_num = '('+phone_num[:3]+')'+phone_num[3:6]+'-'+phone_num[6:]
    return new_phone_num

# %%
df2 = df2.withColumn('BRANCH_PHONE',phone_num2('BRANCH_PHONE'))

# %%


# %%


# %%
# Change for cdw_sapp_credit
df3 = spark.read.option('multiline',True).load("cdw_sapp_credit.json",format='json')

# %%


# %%


# %%
# DAY, MONTH, YEAR:Convert DAY, MONTH, and YEAR into a TIMEID (YYYYMMDD)
@udf
def date_change(date):
    if date < 10:
        return '0'+str(date)
    return date

df3=df3.withColumn('MONTH',date_change('MONTH'))
df3=df3.withColumn('DAY',date_change('DAY'))
df3=df3.withColumn('TIMEID',concat(col('YEAR'),col('MONTH'),col('DAY')))
df3=df3.drop('YEAR','MONTH','DAY')


# %%



# %%
# crate and load data into MYSQL
df.write.format("jdbc") \
    .mode("append") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", "root") \
    .option("password", "password") \
    .save()

df2.write.format("jdbc") \
    .mode("append") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_BRANCH") \
    .option("user", "root") \
    .option("password", "password") \
    .save()


df3.write.format("jdbc") \
    .mode("append") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
    .option("user", "root") \
    .option("password", "password") \
    .save()

# %%
#2 Functional Requirements - Application Front-End

# %%
#2.1 Create a function that accomplishes the following tasks:
def get_zip():
    zip = input("Please enter a five-digit the zip code:")
    if len(zip) == 5 and zip.isnumeric():
        print("Valid zip code.")
        return zip
    print("Invalid zipcode.")

def month_year():
    month = input("Please enter a month(1-12):")
    if int(month) in range(1,13):
        year = input("Please enter a year:")
        if len(year) == 4 and year.isnumeric():
            print('Valid format')
            return month, year
    print('Invalid format')
    
def query_date_zip(zip,month,year):
    return df.join(df3, df.SSN == df3.CUST_SSN,'inner').filter(df.CUST_ZIP==zip).filter(df3.TIMEID<=year+month+'31').filter(df3.TIMEID >= year+month+'01')
    
def sort_by_day(df):
    return df.sort(col('TIMEID').desc())

# %%


# %% [markdown]
# 

# %%
# 2.2

# %%
def customer_details(credit_no):   
    return df.filter(df.CREDIT_CARD_NO==str(credit_no))

from pyspark.sql.functions import when
from pyspark.sql.functions import current_timestamp

def modify_customer(credit_no):
    global df
    choice = ''
    
    
    while choice.lower() != 'q':
        print("1.Update SSN")
        print("2.Update First Name")
        print("3.Update Middle Name")
        print("4.Update Last Name")
        print("5.Update Credit Card No")
        print("6.Update Full Street Adress")
        print("7.Update Customer City")
        print("8.Update Customer State")
        print("9.Update Customer Country")
        print("10.Update Customer Zip")
        print("11.Update Customer Phone")
        print("12.Update Customer Email")

        print("Enter q to quit")

        choice =input("Enter your selection:")
        if choice.lower() != 'q':
             
            new_input = input('Enter new value:')

        if choice == '1':
            df=df.withColumn('SSN',when(col('CREDIT_CARD_NO') == credit_no,int(new_input)).otherwise(col('SSN')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
            update = True
        elif choice == '2':
            df=df.withColumn('FIRST_NAME',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('FIRST_NAME')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
        elif choice == '3':
            df=df.withColumn('MIDDLE_NAME',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('MIDDLE_NAME')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
        elif choice == '4':
            df=df.withColumn('LAST_NAME',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('LAST_NAME')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
        elif choice == '5':
            df=df.withColumn('CREDIT_CARD_NO',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('CREDIT_CARD_NO')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            credit_no=new_input
            df.show()
        elif choice == '6':
            df=df.withColumn('FULL_STREET_ADDRESS',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('FULL_STREET_ADDRESS')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
        elif choice == '7':
            df=df.withColumn('CUST_CITY',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('CUST_CITY')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
        elif choice == '8':
            df=df.withColumn('CUST_STATE',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('CUST_STATE')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
        elif choice == '9':
            df=df.withColumn('CUST_COUNTRY',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('CUST_COUNTRY')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
        elif choice == '10':
            df=df.withColumn('CUST_ZIP',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('CUST_ZIP')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
        elif choice == '11':
            df=df.withColumn('CUST_PHONE',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('CUST_PHONE')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
        elif choice == '12':
            df=df.withColumn('CUST_EMAIL',when(col('CREDIT_CARD_NO') == credit_no,new_input).otherwise(col('CUST_EMAIL')))
            df=df.withColumn('LAST_UPDATED',when(col('CREDIT_CARD_NO') == credit_no,current_timestamp()).otherwise(col('LAST_UPDATED')))
            df.show()
        else:
            print("Please enter a valid choice")
            


def monthly_bill(year,month,credit_no):
    return df3.filter(df3.CREDIT_CARD_NO==str(credit_no)).filter(df3.TIMEID<=year+month+'31').filter(df3.TIMEID >= year+month+'01').groupBy('CUST_SSN').sum().collect()[0][-1]

    
def transactions_between_dates(credit_no,date1,date2):
    return df3.filter(df3.CREDIT_CARD_NO==str(credit_no)).filter(df3.TIMEID<=date2).filter(df3.TIMEID >= date1).sort(col('TIMEID').desc())

# %%
from pyspark.sql.functions import current_timestamp
print(current_timestamp())

# %%


# %%


# %%


# %%
# Create front-end interface
choice = ''
while choice.lower() != 'q':
        print("1.Verify zipcode")
        print("2.Verify Month and Year")
        print("3.Query Transactions by zipcode, month and year")
        print("4.Sort Transactions by day")
        print("5.Check Account Details")
        print("6.Modify Account Details")
        print("7.Generate a Monthly Bill")
        print("8.Display transactions of a customer between two days")
        print("Enter q to quit")

        choice =input("Enter your selection:")

        if choice == '1':
                get_zip()
        elif choice == '2':
                month_year()
        elif choice == '3':
                zip = input("Please enter a zip code:")
                month = input("Please enter a month:")
                year = input("Please enter a year:")
                df_by_query=query_date_zip(zip,month,year)
                df_by_query.show()
        elif choice == '4':
                if df_by_query:
                     sort_by_day(df_by_query).show()
                else:
                     print("Please run a query(option 3) first")
        elif choice == '5':
                credit_no = input("Please enter a credit card number:")
                customer_details(credit_no).show()
        elif choice == '6':
                credit_no = input("Please enter a credit card number:")
                modify_customer(credit_no)
        elif choice == '7':
                year = input("Please enter a year:")
                month = input("Please enter a month:")
                credit_no = input("Please enter a credit card number:")
                print(monthly_bill(year,month,credit_no))
        elif choice == '8':
                credit_no = input("Please enter a credit card number:")
                date1 = input("Please enter the first date:")
                date2 = input("Please enter the second date:")
                transactions_between_dates(credit_no,date1,date2).show()
        elif choice.lower() != "q":
                print('Enter a valid choice.')

# %%
# 3. Functional Requirements - Data Analysis and Visualization

# %%
import matplotlib.pyplot as plt
import seaborn as sns
#1
#%matplotlib inline 

# %%
# convert pyspark dataframe to a pandas dataframe
df_3_1 = df3.toPandas()
# sort my data by transaction_id count
df_3_1 = df_3_1.groupby(['TRANSACTION_TYPE'])['TRANSACTION_ID'].count().reset_index(name='count').sort_values(['count'], ascending=False)

# %%
plt.figure(figsize=(8,6))
ax=sns.barplot(x='TRANSACTION_TYPE',y='count',hue='TRANSACTION_TYPE',data=df_3_1)
plt.xlabel('Transaction Type')
plt.ylabel('Transactions')
plt.title('Number of Transactions by Transaction Type')
# change my y range to make the x values easy to compare
ax.set_ylim([6000, 7000])
# add labels for x
for rect in ax.patches:
    # Find where everything is located
    height = rect.get_height()
    width = rect.get_width()
    x = rect.get_x()
    y = rect.get_y()
    
    # change labels to integer
    label_text = int(height)
    
    # ax.text(x, y, text)
    label_x = x + width / 2
    label_y = y + height *1.005
    ax.text(label_x, label_y, label_text, ha='center', va='center')



plt.show()

# %%
df_3_2 = df.toPandas()
# sort my data by transaction_id count
df_3_2 = df_3_2.groupby(['CUST_STATE'])['SSN'].count().reset_index(name='count').sort_values(['count'], ascending=False).head(10)

# %%
df_3_2

# %%
plt.figure(figsize=(8,6))
ax=sns.barplot(x='CUST_STATE',y='count',palette='crest_r', data=df_3_2)
plt.xlabel('States')
plt.ylabel('Customers')
plt.title('Top 10 States With Highest Number of Customers')
# change my y range to make the x values easy to compare
ax.set_ylim([0, 102])
# add labels for x
for rect in ax.patches:
    # Find where everything is located
    height = rect.get_height()
    width = rect.get_width()
    x = rect.get_x()
    y = rect.get_y()
    
    # change labels to integer
    label_text = int(height)
    
    # ax.text(x, y, text)
    label_x = x + width / 2
    label_y = y + height *1.04
    ax.text(label_x, label_y, label_text, ha='center', va='center')



plt.show()

# %%
# join customer and credit card tables by ssn
df_3_3=df.join(df3, df.SSN == df3.CUST_SSN,'inner')
df_3_3=df_3_3.toPandas()
# group by customers and sum trasaction amount, sort and find the top 10 customers
df_3_3 = df_3_3.groupby(['SSN','FIRST_NAME'])['TRANSACTION_VALUE'].sum().reset_index(name='transaction amount').sort_values(['transaction amount'], ascending=False).head(10)

# %%
plt.figure(figsize=(8,6))
ax=sns.barplot(x='FIRST_NAME',y='transaction amount',palette='YlOrBr_r', data=df_3_3)
plt.xlabel('Customers')
plt.ylabel('Transaction Values')
plt.title('Top 10 Customers With Highest Transaction Amounts')
# change my y range to make the x values easy to compare
ax.set_ylim([4000, 6000])
# add labels for x
for rect in ax.patches:
    # Find where everything is located
    height = rect.get_height()
    width = rect.get_width()
    x = rect.get_x()
    y = rect.get_y()
    
    # change labels to integer
    label_text = int(height)
    
    # ax.text(x, y, text)
    label_x = x + width / 2
    label_y = y + height *1.01
    ax.text(label_x, label_y, label_text, ha='center', va='center')



plt.show()

# %%
# 4.

# %%
import requests
r = requests.get('https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json')
#loan_data = r.text
rdd = spark.sparkContext.parallelize([r.text])
#r.status_code

# %%
df4 = spark.read.json(rdd)

# %%
df4.show()

# %%
# load loan dataset to MySQL database
df4.write.format("jdbc") \
    .mode("append") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_loan_application") \
    .option("user", "root") \
    .option("password", "password") \
    .save()


