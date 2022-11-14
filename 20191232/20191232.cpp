#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <mysql.h>


#pragma comment(lib, "libmysql.lib")


MYSQL_RES* sql_result;
MYSQL_ROW sql_row;
const char* host = "localhost";
const char* user = "root";
const char* pw = "opop0808!!@@";
const char* db = "project2";
const int MAX = 1000;

void query1(MYSQL * connection);
void query1_1(MYSQL* connection, int p_id, int purchased_date, int Tracking_number);
void query2(MYSQL* connection);
void query2_1(MYSQL* connection);
void query3(MYSQL* connection);
void query3_1(MYSQL* connection);
void query3_2(MYSQL* connection);
void query4(MYSQL* connection);
void query4_1(MYSQL* connection);
void query4_2(MYSQL* connection);
void query5(MYSQL* connection);
void query6(MYSQL* connection);
void query7(MYSQL* connection);
void delete_semicolon(char* arr);

int main(void) {

	char query[MAX];
	int state = 0; int type = 0;
	FILE* file = fopen("20191232.txt", "r");
	if (file == NULL) {
		printf("No text file.");
		return 0;
	}
	
	MYSQL* connection = NULL;
	MYSQL conn;


	if (mysql_init(&conn) == NULL)
		printf("mysql_init() error!");
	connection = mysql_real_connect(&conn, host, user, pw, db, 3306, (const char*)NULL, 0);
	if (connection == NULL)
	{
		printf("%d ERROR : %s\n", mysql_errno(&conn), mysql_error(&conn));
		return 1;
	}
	else
	{
		printf("Connection Succeed\n");

		if (mysql_select_db(&conn, db))
		{
			printf("%d ERROR : %s\n", mysql_errno(&conn), mysql_error(&conn));
			return 1;
		}


		
		while (!feof(file)) {
			fgets(query, MAX, file);
			delete_semicolon(query);
			if (query[0] == 'd' && query[29] == 'a') {
				while (1) {
					printf("---------- SELECT QUERY TYPES ----------\n");
					printf("\n");
					for (int i = 1; i < 8; i++) printf("%d. TYPE %d\n", i, i);
					printf("0. Quit\n");
					scanf("%d", &type);
					if (type == 1) query1(connection);
					else if (type == 2) query2(connection);
					else if (type == 3) query3(connection);
					else if (type == 4) query4(connection);
					else if (type == 5) query5(connection);
					else if (type == 6) query6(connection);
					else if (type == 7) query7(connection);
					else if (type == 0) break;
					else printf("Input correct TYPE number\n");
				}
			}
			mysql_query(connection, query);		
		}		
	}
	
	//mysql_free_result(sql_result);
	mysql_close(connection);
	return 0;
}


void query1(MYSQL* connection) {
	
	char update[MAX];
	int sub_type = 0, X = 0, state = 0, p_id = 0, purchased_date = 0;
	printf("------- TYPE 1 -------\n");
	printf("\n");
	printf("** Assume the package shipped by USPS with tracking number X is reported to have been destroyed in an accident.Find the contact information for the customer. **\n");
	printf("Which  X? : ");
	scanf("%d", &X);
	printf("\n");
	sprintf(update, "SELECT * FROM online_customer_purchased_product JOIN customer using (c_id) join sales_data using(p_id,purchased_date) where Tracking_number = %d order by p_id", X);
	mysql_query(connection, update);
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		printf("customer id : %d, customer name :%s, customer phone number : %s\n", atoi(sql_row[2]), sql_row[6], sql_row[7]);
		p_id = atoi(sql_row[0]); purchased_date = atoi(sql_row[1]);
	}
	sprintf(update, "update online_customer_purchased_product set status = 'X' where Tracking_number=%d", X);
	mysql_query(connection, update);
	sprintf(update, "update sales_data set delivered_date = 0 where (p_id = %d and purchased_date=%d);", p_id, purchased_date);
	mysql_query(connection, update);
	while (1) {
		printf("\n");
		printf("---------- Subtypes in TYPE 1 ----------\n");
		printf("\n");
		printf("1. TYPE 1-1\n");
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 1) query1_1(connection, p_id, purchased_date, X);
		else if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}

}
void query1_1(MYSQL* connection, int p_id, int purchased_date, int Tracking_number) {
	char update[MAX]; char select[MAX];
	int sub_type = 0; int delivered_date = purchased_date + 21;
	printf("------- TYPE 1-1 -------\n");
	printf("\n");
	printf("** Then find the contents of that shipment and create a new shipment of replacement items. **\n");
	sprintf(select, "select Product, p_id from product_data where p_id = %d", p_id);
	mysql_query(connection, select);
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		printf("\n");
		printf("Product : %s\n", sql_row[0]);		
	}
	sprintf(update, "update online_customer_purchased_product set status = 'O' where Tracking_number=%d", Tracking_number);
	mysql_query(connection, update);
	sprintf(update, "update sales_data set delivered_date = %d where (p_id = %d and purchased_date=%d);", delivered_date, p_id, purchased_date);
	mysql_query(connection, update);
	mysql_free_result(sql_result);
	while (1) {
		printf("\n");
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query2(MYSQL* connection) {
	int sub_type = 0;
	printf("------- TYPE 2 -------\n");
	printf("\n");
	printf("**  Find the customer who has bought the most (by price) in the past year. **\n");
	mysql_query(connection, "with query_2 as (select c_id, concat(ifnull(online_customer_purchased_product.p_id,''), ifnull(contract_customer_purchased_bill.p_id,'')) as p_id, name, ifnull(online_customer_purchased_product.bill,0)+ifnull(contract_customer_purchased_bill.bill,0) as bill, concat(ifnull(online_customer_purchased_product.purchased_date,''), ifnull(contract_customer_purchased_bill.purchased_date,'')) as date from customer left join online_customer_purchased_product using (c_id) left join contract_customer_purchased_bill using (c_id)) select c_id, name, sum(bill) from query_2 where (date>20210000 and date<20220000) group by c_id order by sum(bill) desc limit 1");
	sql_result = mysql_store_result(connection);
	printf("This year is 2022. past year is 2021.\n");
	printf("\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		printf("c_id : %d,  Name : %s, Bill : %d\n", atoi(sql_row[0]), sql_row[1], atoi(sql_row[2]));

	}
	while (1) {
		printf("\n");
		printf("---------- Subtypes in TYPE 2 ----------\n");
		printf("\n");
		printf("1. TYPE 2-1\n");
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 1) query2_1(connection);
		else if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query2_1(MYSQL* connection) 
{
	int sub_type = 0;
	printf("------- TYPE 2-1 -------\n");
	printf("\n");
	printf("** Then find the product that the customer bought the most. **\n");
	printf("\n");
	mysql_query(connection, "with query_2 as (select c_id, concat(ifnull(online_customer_purchased_product.p_id,''), ifnull(contract_customer_purchased_bill.p_id,'')) as p_id, name, ifnull(online_customer_purchased_product.bill,0)+ifnull(contract_customer_purchased_bill.bill,0) as bill, concat(ifnull(online_customer_purchased_product.purchased_date,''), ifnull(contract_customer_purchased_bill.purchased_date,'')) as date from customer left join online_customer_purchased_product using (c_id) left join contract_customer_purchased_bill using (c_id)) select c_id, name, p_id, product, bill from query_2 join product_data using(p_id) where(date>20210000 and date<20220000 and c_id=(select c_id from query_2 where (date>20210000 and date<20220000) group by c_id order by sum(bill) desc limit 1))");
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		printf("product : %s, Bill : %d\n", sql_row[3], atoi(sql_row[4]));
	}
	while (1) {
		printf("\n");
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query3(MYSQL* connection) {
	int sub_type = 0;
	printf("------- TYPE 3 -------\n");
	printf("\n");
	printf("** Find all products sold in the past year. **\n");
	mysql_query(connection, "with query_3 as (select c_id, concat(ifnull(online_customer_purchased_product.p_id,''), ifnull(contract_customer_purchased_bill.p_id,'')) as p_id, name, ifnull(online_customer_purchased_product.bill,0)+ifnull(contract_customer_purchased_bill.bill,0) as bill, concat(ifnull(online_customer_purchased_product.purchased_date,''), ifnull(contract_customer_purchased_bill.purchased_date,'')) as date from customer left join online_customer_purchased_product using (c_id) left join contract_customer_purchased_bill using (c_id)) select Product from query_3  left join sales_data using(p_id) left join product_data using(p_id) where(date=purchased_date and date>20210000 and date<20220000) group by p_id order by Product;");
	printf("This year is 2022. past year is 2021.\n");
	printf("\n");
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		printf("Product : %s\n", sql_row[0]);

	}
	while (1) {
		printf("\n");
		printf("---------- Subtypes in TYPE 3 ----------\n");
		printf("\n");
		printf("1. TYPE 3-1\n");
		printf("2. TYPE 3-2\n");
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 1) query3_1(connection);
		else if (sub_type == 2) query3_2(connection);
		else if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query3_1(MYSQL* connection) {
	int sub_type = 0, k = 0, count = 0;
	printf("------- TYPE 3-1 -------\n");
	printf("\n");
	printf("** Then find the top k products by dollar-amount sold. **\n");
	printf("Which k? : ");
	scanf("%d", &k);
	mysql_query(connection, "with query_3 as (select c_id, concat(ifnull(online_customer_purchased_product.p_id,''), ifnull(contract_customer_purchased_bill.p_id,'')) as p_id, name, ifnull(online_customer_purchased_product.bill,0)+ifnull(contract_customer_purchased_bill.bill,0) as bill, concat(ifnull(online_customer_purchased_product.purchased_date,''), ifnull(contract_customer_purchased_bill.purchased_date,'')) as date from customer left join online_customer_purchased_product using (c_id) left join contract_customer_purchased_bill using (c_id)) select count(p_id), p_id, Product, sum(price) from query_3  left join sales_data using(p_id) left join product_data using(p_id) where(date=purchased_date and date>20210000 and date<20220000) group by Product order by sum(price) desc");
	sql_result = mysql_store_result(connection);
	printf("\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		if (k == 0) break;
		count++;
		printf("Product : %s, Sum of price : %d\n", sql_row[2], atoi(sql_row[3]));
		if (k == count) break;
	}
	if (k > count) {
		printf("Ignore k = %d, since the number of products is %d.\n", k, count);
	}
	while (1) {
		printf("\n");
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query3_2(MYSQL* connection) {
	int sub_type = 0, count = 0, percent = 0;
	printf("------- TYPE 3-2 -------\n");
	printf("\n");
	printf("** And then find the top 10%% products by dollar-amount sold. **\n");
	mysql_query(connection, "with query_3 as (select c_id, concat(ifnull(online_customer_purchased_product.p_id,''), ifnull(contract_customer_purchased_bill.p_id,'')) as p_id, name, ifnull(online_customer_purchased_product.bill,0)+ifnull(contract_customer_purchased_bill.bill,0) as bill, concat(ifnull(online_customer_purchased_product.purchased_date,''), ifnull(contract_customer_purchased_bill.purchased_date,'')) as date from customer left join online_customer_purchased_product using (c_id) left join contract_customer_purchased_bill using (c_id)) select count(p_id), p_id, Product, sum(price) from query_3  left join sales_data using(p_id) left join product_data using(p_id) where(date=purchased_date and date>20210000 and date<20220000) group by Product order by sum(price) desc");
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL){count++;}
	percent = count / 10;
	mysql_query(connection, "with query_3 as (select c_id, concat(ifnull(online_customer_purchased_product.p_id,''), ifnull(contract_customer_purchased_bill.p_id,'')) as p_id, name, ifnull(online_customer_purchased_product.bill,0)+ifnull(contract_customer_purchased_bill.bill,0) as bill, concat(ifnull(online_customer_purchased_product.purchased_date,''), ifnull(contract_customer_purchased_bill.purchased_date,'')) as date from customer left join online_customer_purchased_product using (c_id) left join contract_customer_purchased_bill using (c_id)) select count(p_id), p_id, Product, sum(price) from query_3  left join sales_data using(p_id) left join product_data using(p_id) where(date=purchased_date and date>20210000 and date<20220000) group by Product order by sum(price) desc");
	sql_result = mysql_store_result(connection);
	printf("The number of products is %d. print %d products\n", count, percent);
	printf("\n");
	count = 0;
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		count++;
		printf("Product : %s, Sum of price : %d\n", sql_row[2], atoi(sql_row[3]));
		if (percent == count) break;
	}
	while (1) {
		printf("\n");
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query4(MYSQL* connection) {
	int sub_type = 0;
	printf("------- TYPE 4 -------\n");
	printf("\n");
	printf("** Find all products by unit sales in the past year. **\n");
	mysql_query(connection, "with query_4 as (select c_id, concat(ifnull(online_customer_purchased_product.p_id,''), ifnull(contract_customer_purchased_bill.p_id,'')) as p_id, name, ifnull(online_customer_purchased_product.bill,0)+ifnull(contract_customer_purchased_bill.bill,0) as bill, concat(ifnull(online_customer_purchased_product.purchased_date,''), ifnull(contract_customer_purchased_bill.purchased_date,'')) as date from customer left join online_customer_purchased_product using (c_id) left join contract_customer_purchased_bill using (c_id)) select count(p_id), p_id, Product from query_4  left join sales_data using(p_id) left join product_data using(p_id) where(date=purchased_date and date>20210000 and date<20220000) group by Product order by count(p_id) desc");
	printf("This year is 2022. past year is 2021.\n");
	printf("\n");
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		printf("Product : %s, unit : %d\n", sql_row[2], atoi(sql_row[0]));

	}
	while (1) {
		printf("\n");
		printf("---------- Subtypes in TYPE 4 ----------\n");
		printf("\n");
		printf("1. TYPE 4-1\n");
		printf("2. TYPE 4-2\n");
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 1) query4_1(connection);
		else if (sub_type == 2) query4_2(connection);
		else if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query4_1(MYSQL* connection) {
	int sub_type = 0, k = 0, count = 0;
	printf("------- TYPE 4-1 -------\n");
	printf("\n");
	printf("** Then find the top k products by unit sales. **\n");
	printf("Which k? : ");
	scanf("%d", &k);
	printf("** Find all products by unit sales in the past year. **\n");
	mysql_query(connection, "with query_4 as (select c_id, concat(ifnull(online_customer_purchased_product.p_id,''), ifnull(contract_customer_purchased_bill.p_id,'')) as p_id, name, ifnull(online_customer_purchased_product.bill,0)+ifnull(contract_customer_purchased_bill.bill,0) as bill, concat(ifnull(online_customer_purchased_product.purchased_date,''), ifnull(contract_customer_purchased_bill.purchased_date,'')) as date from customer left join online_customer_purchased_product using (c_id) left join contract_customer_purchased_bill using (c_id)) select count(p_id), p_id, Product from query_4  left join sales_data using(p_id) left join product_data using(p_id) where(date=purchased_date and date>20210000 and date<20220000) group by Product order by count(p_id) desc");
	printf("\n");
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		if (k == 0) break;
		count++;
		printf("Product : %s, unit : %d\n", sql_row[2], atoi(sql_row[0]));
		if (k == count) break;
	}
	if (k > count) {
		printf("Ignore k = %d, since the number of products is %d.\n", k, count);
	}
	while (1) {
		printf("\n");
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query4_2(MYSQL* connection) {
	int sub_type = 0, count = 0, percent = 0;
	printf("------- TYPE 4-2 -------\n");
	printf("\n");
	printf("** And then find the top 10%% products by unit sales.  **\n");
	mysql_query(connection, "with query_4 as (select c_id, concat(ifnull(online_customer_purchased_product.p_id,''), ifnull(contract_customer_purchased_bill.p_id,'')) as p_id, name, ifnull(online_customer_purchased_product.bill,0)+ifnull(contract_customer_purchased_bill.bill,0) as bill, concat(ifnull(online_customer_purchased_product.purchased_date,''), ifnull(contract_customer_purchased_bill.purchased_date,'')) as date from customer left join online_customer_purchased_product using (c_id) left join contract_customer_purchased_bill using (c_id)) select count(p_id), p_id, Product from query_4  left join sales_data using(p_id) left join product_data using(p_id) where(date=purchased_date and date>20210000 and date<20220000) group by Product order by count(p_id) desc");
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL) count++;
	percent = count / 10;
	printf("The number of products is %d. print %d products\n", count, percent); count = 0;
	mysql_query(connection, "with query_4 as (select c_id, concat(ifnull(online_customer_purchased_product.p_id,''), ifnull(contract_customer_purchased_bill.p_id,'')) as p_id, name, ifnull(online_customer_purchased_product.bill,0)+ifnull(contract_customer_purchased_bill.bill,0) as bill, concat(ifnull(online_customer_purchased_product.purchased_date,''), ifnull(contract_customer_purchased_bill.purchased_date,'')) as date from customer left join online_customer_purchased_product using (c_id) left join contract_customer_purchased_bill using (c_id)) select count(p_id), p_id, Product from query_4  left join sales_data using(p_id) left join product_data using(p_id) where(date=purchased_date and date>20210000 and date<20220000) group by Product order by count(p_id) desc");
	printf("\n");
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		count++;
		printf("Product : %s, unit : %d\n", sql_row[2], atoi(sql_row[0]));
		if (percent == count) break;
	}
	if (percent > count) {
		printf("Ignore k = %d, since the number of products is %d.\n", percent, count);
	}
	while (1) {
		printf("\n");
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query5(MYSQL* connection) {
	int sub_type = 0; int p_id = 0;
	char update[MAX];
	printf("------- TYPE 5 -------\n");
	printf("\n");
	printf("** Find those products that are out-of-stock at every store in California. **\n");
	printf("\n");
	mysql_query(connection, " SELECT *from stores join product_data using(p_id) where (inventory = 0 and region = 'California')");
	sql_result = mysql_store_result(connection);
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		printf("store name : %s, Product :%s\n", sql_row[1], sql_row[4]);
		p_id = atoi(sql_row[0]);
		sprintf(update, "update stores set inventory = 100 where (p_id = %d and region = 'California')", p_id);
		mysql_query(connection, update);
	}
	while (1) {
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query6(MYSQL* connection) {
	int sub_type = 0;
	printf("------- TYPE 6 -------\n");
	printf("\n");
	printf("** Find those packages that were not delivered within the promised time. **\n");
	mysql_query(connection, "SELECT * FROM sales_data join product_data using(p_id) where((delivered_date-purchased_date)>7)");
	sql_result = mysql_store_result(connection);
	printf("Packages are delivered within 7 days\n");
	printf("\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		printf("Prouduct : %s, Purchased day :%d, Delivered day :%d\n", sql_row[4], atoi(sql_row[2]), atoi(sql_row[3]));

	}
	while (1) {
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}
void query7(MYSQL* connection) {
	int sub_type = 0;
	printf("------- TYPE 7 -------\n");
	printf("\n");
	printf("** Generate the bill for each customer for the past month. **\n");
	mysql_query(connection, "with query_7 as(SELECT * FROM contract_customer_purchased_bill join sales_data using(purchased_date, p_id) join customer using(c_id) where (purchased_date>20220400 and purchased_date<20220600)) select c_id, name, sum(bill) from query_7 group by name");
	sql_result = mysql_store_result(connection);
	printf("This month is May, 2022. We find the date in Aprill.\n");
	printf("\n");
	while ((sql_row = mysql_fetch_row(sql_result)) != NULL)
	{
		printf("c_id : %d, Name : %s, Bill :%d\n", atoi(sql_row[0]), sql_row[1], atoi(sql_row[2]));

	}
	while (1) {
		printf("0. Quit\n");
		scanf("%d", &sub_type);
		if (sub_type == 0) break;
		else printf("Input correct TYPE number\n");
	}
}

void delete_semicolon(char* arr) {
	for (int i = 0; i < MAX; i++) {
		if (arr[i] == ';') {
			arr[i] = '\0';
		}
	}
}