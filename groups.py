from flask import Flask, Response, url_for, redirect, make_response, abort, request, jsonify, render_template, json
# from sqlalchemy import create_engine
from flask_restful import Resource, Api
from collections import defaultdict
from itertools import combinations
import gurobipy 
import os
from collections import defaultdict
import httplib
import csv
import glob
import sys
from celery import Celery
import time
from flask.ext.mysql import MySQL


#create flask app
app = Flask(__name__)

#mysql configuration

mysql = MySQL()
 
# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = ''
app.config['MYSQL_DATABASE_PASSWORD'] = ''
app.config['MYSQL_DATABASE_DB'] = 'Jigsaw'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'
mysql.init_app(app)


app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'# URL tells Celery where the broker service is running, if the  broker on a different machine, then you will need to change the URL accordingly.
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
celery = Celery(app.name, broker = app.config['CELERY_BROKER_URL']) # initialize the Celery client
celery.conf.update(app.config)


# improved JSON 404 error handler:
@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'Bad Request'}), 400)



@app.route('/api/v1.0/groups', methods = ['POST'])
def groupAllocation():


	if not request.json: #check format based on FROG input - should be int
		abort(400)
		return
	if not 'number_of_tasks' in request.json: #check format based on FROG input - should be int
		abort(400)
		return

	if not 'number_of_groups' in request.json: #check format based on FROG input - should be int
		abort(400)
		return

	if not 'min_stu_per_groups' in request.json: #check format based on FROG input - should be int
		abort(400)
		return

	if not 'option' in request.json: #check format based on FROG input - should be int
		abort(400)
		return

	if not 'attribute' in request.json: #check format based on FROG input - should be int
		abort(400)
		return

	else:


		contents = request.get_json()

		tasks_count = request.json.get('number_of_tasks')
		tasks_count = range(1, int(tasks_count)+1)

		group_count = request.json.get('number_of_groups')
		groups = range(1, int(group_count)+1)

		minStudentsPerGroup = request.json.get('min_stu_per_groups')

		opt = request.json.get('option') # opt1= heterogeneous opt2 = homogeneous

		attribute = request.json.get('attribute')

		task = groupAllocation.delay(contents = contents, tasks_count = tasks_count, groups = groups,  minStudentsPerGroup = minStudentsPerGroup, opt = opt, attribute = attribute) #server starts the task, and stores the return value
		# Flask application  request the execution of the background task using delay() can also use apply_async() to give more power

		# print task.id - The return value of delay() and apply_async() is an object that represents the task, and this object can be used to obtain status
		return jsonify({ 'id': task.id }), 202,  {'Location': url_for('groupAllocStatus',task_id=task.id)} 



@celery.task()
def groupAllocation(contents, tasks_count, groups, minStudentsPerGroup, opt, attribute):
	d = contents['students_data']

	attr_info = {i["id"]:i[attribute] for i in d}

	new_stu = attr_info.keys()

	print 'Preprocessing...'

	cost = defaultdict(int)
	if opt == 1:
		for k1,k2 in combinations(attr_info,2):
			if attr_info[k1] == attr_info[k2]:
				cost[(k1,k2)] += 1
	if opt == 2:
		for k1,k2 in combinations(attr_info,2):
			if attr_info[k1] != attr_info[k2]:
				cost[(k1,k2)] += 1

	jigsaw_allocation = {}

	con = mysql.connect()
	cursor = con.cursor()

	query1 = "select * from task_allocation"
	try:
		cursor.execute(query1)
	except (MySQLdb.Error, MySQLdb.Warning) as e:
		print(e)
		return None

	t_alloc = defaultdict(int)

	for rows in cursor.fetchall():
		id, tasks = rows
		t_alloc [(id,tasks)] += 1
	print "t_alloc", t_alloc #previous task allocation details
	
	m = gurobipy.Model("jigsawmodel") # model definitions

	galloc = {}

	for s in new_stu:
		for k in groups:
			galloc[s,k] = m.addVar(vtype = gurobipy.GRB.BINARY, name = "galloc") #decision variable
	m.update()

	obj_intermediate = 0 
	for s in new_stu:
		for j in new_stu:
			for k in groups:
				obj_intermediate += galloc[s,k] * galloc[s,k] * cost[s,j] 
	m.setObjective(obj_intermediate, gurobipy.GRB.MINIMIZE)

	for s in new_stu:
		m.addConstr(gurobipy.quicksum(galloc[s,k] for k in groups) == 1) #constraint 1
	for k in groups:
		m.addConstr(gurobipy.quicksum(galloc[s,k] for s in new_stu ) >= minStudentsPerGroup) #constraint 2
	for t in tasks_count:
		for k in groups:
			m.addConstr(gurobipy.quicksum(t_alloc[s,t] * galloc[s,k] for s in new_stu) >= 1) #constraint 3
	m.optimize()


	query2 = "DROP TABLE IF EXISTS group_allocation"
	try:
		cursor.execute(query2)
	except (MySQLdb.Error, MySQLdb.Warning) as e:
		print(e)
		return None
	

	query3 = "create table group_allocation (stu_id VARCHAR(50), group_id INT)" 
	try:
		cursor.execute (query3)
	except (MySQLdb.Error, MySQLdb.Warning) as e:
		print(e)
		return None

	if m.status == gurobipy.GRB.status.OPTIMAL:
		print '\Optimal group Allocations :'
		for s in new_stu:
			for k in groups:
				if galloc[s,k].x == 1.0:
					print s,k
					try:
						cursor.execute("INSERT INTO group_allocation VALUES (%s,%s)",[s,k])
					except (MySQLdb.Error, MySQLdb.Warning) as e:
						print(e)
						return None
		con.commit()
		con.close()

#show results
	
	con = mysql.connect()
	cursor = con.cursor()
	result = cursor.execute("select stu_id, group_id  from group_allocation order by group_id")
	data = cursor.fetchall()

	v={}

	for key, value in data:
		v.setdefault(value, []).append(key)
	print v
	con.close()
	return json.dumps({'groups':v},separators=(',', ': '))

@app.route('/status/<task_id>') #obtain algorithm processed results
def groupAllocStatus(task_id):
    task = groupAllocation.AsyncResult(task_id)
    response = {'state': task.state, 'status': str(task.info)}
    return jsonify(response)
 
if __name__ == '__main__':  #check if the executed file is the main program and run the app:
	# sess = Session()
	app.run(debug=True)