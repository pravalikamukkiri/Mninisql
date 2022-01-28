import pandas as pd
import re
import csv
import numpy
import sys

schema = []

dir = 'files'

def display(data):
	try:
		maxl=0
		for i in range(len(data)):
			maxl=max(maxl,len(data[i]))
		for i in range(maxl):
			for j in range(len(data)):
				if(i<len(data[j])):
					print(data[j][i],end="")
				print("\t",end="")
			print("\n",end="")
	except:
		print("error in displaying data")
		return

def read_metafile():
	try:
		metafile = open('files/metadata.txt','r')
		data = metafile.readlines()
		temp_table=[]
		flag=0
		for line in data:
			line=line[0:len(line)-1]
			if(line=="<begin_table>"):
				flag=1
				continue
			if(line=='<end_table>'):
				flag=0
				schema.append(temp_table)
				temp_table=[]
				continue
			if(flag==1):
				temp_table.append(line.lower())
		# print(schema)
	except:
		print("error in reading metafile")



def read_data():
	read_metafile()

def minimum(data):
	try:
		data_p=[int(x) for x in data]
		return min(data_p)
	except:
		print("error in finding minimum")
		return

def maximum(data):
	try:
		data_p=[int(x) for x in data]
		return max(data_p)
	except:
		print("error in finding maximum")

def summation(data):
	try:
		data_p=[int(x) for x in data]
		return sum(data_p)
	except:
		print("error in summation of data")
		return

def select_Columns(col_names, table_names):
	# print("select_Columns")
	try:
		if(len(col_names)==1 and col_names[0]=='*'):
			data = read_table(table_names[0])
			return data
			# display(data)
		elif(len(table_names)==1):
			data = read_colums(col_names,table_names[0])
			return data
			# display(data)

		else:
			data_f=[]
			cols_f=[]
			for x in col_names:
				fg=0
				for y in schema:
					if(x in y):
						fg=1
				if(fg==0):
					print("fSyntax error")
					return
			data=[[] for i in range(len(col_names))]
			for table in table_names:
				# print(table)
				cols=[]
				for x in schema:
					if(x[0]==table):
						for col in col_names:
							if(col in x):
								cols.append(col)
								cols_f.append(col)
				datat = read_colums(cols,table)
				# print(datat)

				data_f=table_join(data_f,datat)				
		# display(data_f)
		data_final=[[] for i in range(len(col_names))]
		for col in col_names:
			c1=cols_f.index(col)
			c2=col_names.index(col)
			data_final[c2]=data_f[c1]
		# display(data_final)
		return data_final



	except:
		print("error in selecting columns")
		return

def table_join(data1,data2):
	# print(data1)
	# print(data2)
	try:
		if(data1==[]):
			return data2
		datar1=[]
		datar2=[]
		for i in range(len(data1[0])):
			temp=[]
			for j in range(len(data1)):
				temp.append(data1[j][i])
			datar1.append(temp)

		for i in range(len(data2[0])):
			temp=[]
			for j in range(len(data2)):
				temp.append(data2[j][i])
			datar2.append(temp)


		dataf=[]
		for x in datar1:
			for y in datar2:
				tl=x+y
				if(tl not in dataf):
					dataf.append(tl)

		# print(dataf)

		data_c=[]
		for i in range(len(dataf[0])):
			temp=[]
			for j in range(len(dataf)):
				temp.append(dataf[j][i])
			data_c.append(temp)

		# print(data_c)
		# display(data_c)
		return data_c
	except:
		print("error in joining tables")
		return

def read_colums(col_names,table):
	try:
		data=[]
		table_name = dir + '/' + table + '.csv'
		# print(table_name)
		# print(col_names)
		col_nums=[]
		for x in schema:
			if(x[0]==table):
				for col in col_names:
					if(col in x):
						col_nums.append(x.index(col)-1)
					else:
						print("Syntax error")
						return
		# print(col_nums)

		data=[[] for i in range(len(col_nums))]
		with open(table_name,'r') as f:
			fp = csv.reader(f)
			for row in fp:
				for col in range(len(row)):
					if(col in col_nums):
						data[col_nums.index(col)].append(row[col])
		return data


	except:
		print("error in reading columns")

def read_table(table):
	try:
		col_names=[]
		for tab in schema:
			if(tab[0]==table):
				col_names=tab[1:]
		data =read_colums(col_names,table)
		# print("do")
		return data
	except:
		print("error while reading table")
		return 


def aggregate_functions(col_name, table_name):
	# print(col_name)
	# print(table_name)
	# print("aggregate_functions")
	try:
		if(col_name[-1]!=')'):
			print("Syntax error")
			return
		func=col_name[0:col_name.index('(')]
		col=col_name[col_name.index('(')+1:-1]

		data=read_colums([col],table_name)
		
		if(func=="max"):
			print(maximum(data[0]))
		elif(func=="min"):
			print(minimum(data[0]))
		elif(func=="count"):
			print(len(data[0]))
		elif(func=="sum"):
			print(summation(data[0]))
		elif(func=="avg"):
			print(summation(data[0])/len(data[0]))
		else:
			print("Syntax error")
			return

	except:
		print("error in aggregate functions")
		return



def distinct(col_names,table_name):
	# print("distinct")
	try:
		data = read_colums(col_names,table_name)
		pairs=[]
		for i in range(len(data[0])):
			pair=[]
			for j in range(len(data)):
				pair.append(data[j][i])
			pairs.append(pair)
		distinct_pairs=[]
		for x in pairs:
			if(x not in distinct_pairs):
				distinct_pairs.append(x)
		# print(distinct_pairs)
		distinct_data=[]
		for j in range(len(distinct_pairs[0])):
			temp=[]
			for i in range(len(distinct_pairs)):
				temp.append(distinct_pairs[i][j])
			distinct_data.append(temp)
		display(distinct_data)

	except:
		print("error in distinct")
		return

def check_condition(cond):
	try:
		condition=""
		check=['<','>','=','<=','>=']
		for x in check:
			if (x in cond):
				condition=x
		# print(condition)
		return condition
	except:
		print("error in checking condition")
		return

def process_where(col_names,table_names,cond):
	try:
		if(len(table_names)>1):
			data_t=[[] for i in range(len(col_names))]
			cols_m=[]
			data_m=[]

			for x in col_names:
				fg=0
				for y in schema:
					if(x in y):
						fg=1
				if(fg==0):
					print("Syntax error")
					return
			cols_t=col_names
			for p in range(len(table_names)):
				# print("\n\n")
				table=table_names[p]
				# print(table)
				cols=[]
				cond_cols=[]
				cond_col_vals=[]
				conds=[]
				conditions=[]
				for x in col_names:
					for y in schema:
						if(y[0]==table and x in y):
							cols.append(x)
							cols_m.append(x)
				# print(cols)

				if "or" in cond:
					condition="or"
					conds=cond.split(condition)
				elif "and" in cond:
					condition="and"
					conds=cond.split(condition)
				else:
					conds=[cond]
				for x in conds:
					cr = check_condition(x)
					col=x.split(cr)[0]
					cnd_val=x.split(cr)[1]
					for y in schema:
						if col in y and y[0]==table:
							cond_cols.append(col)
							cond_col_vals.append(cnd_val)
							conditions.append(cr)
				# print(cond_cols)
				# print(cond_col_vals)
				# print(conditions)
				data=[]
				if(len(conditions)==0):
					data=read_colums(cols,table)
					# print(data)
				elif(len(conditions)==1):
					data=read_columswithcond("and",conditions,cond_col_vals,cols,cond_cols,table)
					# print(data)
				elif(len(conditions)==2):
					data=read_columswithcond(condition,conditions,cond_col_vals,cols,cond_cols,table)
				# print(data)
				data_m=table_join(data_m,data)

			data_final=[[] for i in range(len(col_names))]
			for i in range(len(col_names)):
				c1=col_names.index(col_names[i])
				c2=cols_m.index(col_names[i])
				data_final[c1]=data_m[c2]
			display(data_final)

		else:
			if "or" in cond:
				conds=cond.split("or")
				cond1=check_condition(conds[0])
				cond2=check_condition(conds[1])
				if(cond1=="" or cond2==""):
					print("Syntax error")
					return
				col_cond1=conds[0].split(cond1)[0]
				cond_val1=conds[0].split(cond1)[1]
				col_cond2=conds[1].split(cond2)[0]
				cond_val2=conds[1].split(cond2)[1]
				data =read_columswithcond("or",[cond1,cond2],[cond_val1,cond_val2],col_names,[col_cond1,col_cond2],table_names[0])
				display(data)
			elif "and" in cond:
				conds=cond.split("and")
				cond1=check_condition(conds[0])
				cond2=check_condition(conds[1])
				if(cond1=="" or cond2==""):
					print("Syntax error")
					return
				col_cond1=conds[0].split(cond1)[0]
				cond_val1=conds[0].split(cond1)[1]
				col_cond2=conds[1].split(cond2)[0]
				cond_val2=conds[1].split(cond2)[1]
				data =read_columswithcond("and",[cond1,cond2],[cond_val1,cond_val2],col_names,[col_cond1,col_cond2],table_names[0])
				display(data)
			else:
				f=0
				condition=check_condition(cond)
				if(condition==""):
					print("Syntax error")
					return
				col_cond=cond.split(condition)[0]
				cond_val=cond.split(condition)[1]

				data =read_columswithcond("and",[condition],[cond_val],col_names,[col_cond],table_names[0])
				display(data)

	except:
		print("error in processing where")
		return


def read_columswithcond(cond,conds,val,col_names,cond_cols,table):
	# print("came")
	# print(conds)
	# print(col_names)
	# print(table)
	# print(val)
	# print(cond)
	try:
		data=[]
		table_name = dir + '/' + table + '.csv'
		col_nums=[]
		cond_col_ind=[]
		for x in schema:
			if(x[0]==table):
				for col in cond_cols:
					if(col in x):
						cond_col_ind.append(x.index(col)-1)
					else:
						print("Syntax error")
						return
				for col in col_names:
					if(col in x):
						col_nums.append(x.index(col)-1)
					else:
						print("Syntax error")
						return

		data=[[] for i in range(len(col_nums))]
		# print(cond_col_ind)
		with open(table_name,'r') as f:
			fp = csv.reader(f)
			for row in fp:
				fg=0
				for i in range(len(conds)):
					x=int(row[cond_col_ind[i]])
					y=int(val[i])
					if(conds[i]=="=" and x==y):
						fg+=1
					elif(conds[i]=="<" and x<y):
						fg+=1
					elif(conds[i]=="<=" and x<=y):
						fg+=1
					elif(conds[i]==">" and x>y):
						fg+=1
					elif(conds[i]==">=" and x>=y):
						fg+=1
				# print(fg)
				for col in range(len(row)):
					if((fg==1 and len(conds)==1) or (fg==1 and len(conds)==2 and cond=="or") or (fg==2 and len(conds)==2)):
						if(col in col_nums):
							data[col_nums.index(col)].append(row[col])

		# display(data)
		return data
	except:
		print("error in reading columns with condition")
		return



def group_by(cols, table,group_col):
	try:
		# print("\n")
		# print(cols)
		# print(table)
		# print(group_col)
		agg_conds=[]
		cols_t=[]
		for x in cols:
			if(x==group_col):
				cols_t.append(x)
				agg_conds.append("none")
			elif( '(' in x and ')' in x):
				cols_t.append(x.split('(')[1][:-1])
				agg_conds.append(x.split('(')[0])
			else:
				print("Syntax error")
				return
		# print(cols_t)
		# print(agg_conds)


		group_col_ind=-1
		for x in schema:
			# print(x)
			if(x[0]==table):
				group_col_ind=x.index(group_col)-1
		# print(group_col_ind)

		data=[[] for i in range(len(cols))]
		group_col_data=[]
		table_name = dir + '/' + table + '.csv'
		with open(table_name,'r') as f:
			fp = csv.reader(f)
			for row in fp:
				for col in range(len(row)):
					if(row[group_col_ind] not in group_col_data):
						group_col_data.append(row[group_col_ind])
			# print("ok")

		for i in range(len(cols)):
			if(cols[i]==group_col):
				data[i]=group_col_data

			else:
				col_ind=-1
				for x in schema:
					if(x[0]==table):
						col_ind = x.index(cols_t[i])-1
				for x in group_col_data:
					data_r=[]
					with open(table_name,'r') as f:
						fp = csv.reader(f)
						for row in fp:
							if(row[group_col_ind]==x):
								data_r.append(row[col_ind])
					# print(data_r)
					if(agg_conds[i]=="max"):
						data[i].append(maximum(data_r))
					elif(agg_conds[i]=="min"):
						data[i].append(minimum(data_r))
					elif(agg_conds[i]=="count"):
						data[i].append(len(data_r))
					elif(agg_conds[i]=="average"):
						data[i].append(summation(data_r)/len(data_r))
					elif(agg_conds[i]=="sum"):
						data[i].append(summation(data_r))


		# print(data)
		display(data)
		return
	except:
		print("error in implementing group by")
		return


def order_by(col_names,table_names,ord_col,order):
	try:
		data=select_Columns(col_names,table_names)
		dataint=[]
		for row in data:
			tp=[]
			for col in row:
				tp.append(int(col))
			dataint.append(tp)
		datanp = numpy.array(dataint)
		for i in range(len(col_names)):
			if(col_names[i]==ord_col):
				if(order==1):
					datanp = datanp[:, datanp[i, :].argsort()]
				else:
					datanp = datanp[:, datanp[i, :].argsort()[::-1]]

		# print("order")
		display(datanp)

	except:
		print("error in inplementing order by")
		return




def prase_query(query):
	# print(query)

	try:
		if "select" not in query:
			print("Incorrect Syntax: No select Keyword found")
			return
		if "from" not in query:
			print("Incorrect Syntax: No from Keyword found")
			return

		query = (re.sub(' +',' ',query)).strip()
		query = query.split(';')
		query = query[0]
		query=query.lower()
		print(query)

		item1 = query.split('from')
		# print(item1)


		item1[0]=(re.sub(' +',' ',item1[0])).strip()

		if "select" not in item1[0]:
			print("Incorrect Syntax: No select Keyword found before from")
			return

		# selecting colums
		items = item1[0][6:]
		items = (re.sub(' +',' ',items)).strip()
		# print(items)

		# columns
		item2 = (re.sub(' ','',items)).strip()
		item2=item2.split(',')
		# print(item2)

		# tables
		item3 = query.split('from')[1]
		item3 = item3.split('where')[0]
		item3 = item3.split('group by')[0]
		item3 = item3.split('order by')[0]
		item3 = (re.sub(' ','',item3)).strip()
		item3 = item3.split(',')
		# print(item3)

		for x in item2:
			if(x==''):
				print("Syntax error")
				return
		for x in item3:
			if(x==''):
				print("Syntax error")
				return


		if "distinct" in items:
			items=items[8:]
			items = (re.sub(' ','',items)).strip()
			# print(items)
			if(len(item3)!=1):
				print("Syntax error")
				return
			col_names=items.split(',')
			distinct(col_names,item3[0])

			return

		if "group by" in query:
			group_col = query.split("group by")[1]
			group_col=(re.sub(' ','',group_col)).strip()
			group_by(item2,item3[0],group_col)
			return

		if "order by" in query:
			x = query.split("order by")[1]
			x=(re.sub(' ','',x)).strip()
			if("asc" in x):
				col=x.split("asc")[0]
				order_by(item2,item3,col,1)
			elif "desc" in x:
				col=x.split("desc")[0]
				order_by(item2,item3,col,-1)
			else:
				col=x.split("asc")[0]
				order_by(item2,item3,col,1)

			return

		if (len(item2)==1 and len(item3)==1):
			if('(' in item2[0] and ')' in item2[0]):
				aggregate_functions(item2[0],item3[0])
				return
			elif('(' in item2[0] or ')' in item2[0]): 
				print("Syntax error")
				return

		if "where" in query:
			cond = query.split("where")[1]
			# print(cond)
			cond = (re.sub(' ','',cond)).strip()
			process_where(item2,item3,cond)
			return	
	
		data =select_Columns(item2,item3)
		display(data)


	except:
		print("error in prasing query")
		return



def main():
	read_data()
	# query=input()
	query = sys.argv[1]
	prase_query(query)


if __name__ == "__main__":
    main()