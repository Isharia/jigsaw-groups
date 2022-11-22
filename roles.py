from flask import Flask, Response, url_for, redirect, request, jsonify, render_template, json, abort, make_response
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
from flask_restful import reqparse

#create flask app
app=Flask(__name__)

#mysql configuration

mysql = MySQL()
 
# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = ''
app.config['MYSQL_DATABASE_PASSWORD'] = ''
app.config['MYSQL_DATABASE_DB'] = 'Jigsaw'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'
mysql.init_app(app)

#celery and redis configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'# connection URL for the broker-where broker is running
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
celery = Celery(app.name, broker = app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


# improved JSON 404 error handler:
@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'Bad Request'}), 400)


@app.route('/api/v1.0/roles', methods = ['POST'])
def roles():

	if not request.json: #check format based on FROG input - should be int
		abort(400)
		return
	if not 'number_of_tasks' in request.json: #check format based on FROG input - should be int
		abort(400)
		return

	if not 'min_stu_per_task' in request.json: #check format based on FROG input - should be int
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

		min_stu_count = request.json.get('min_stu_per_task')

		opt = request.json.get('option') # opt1= heterogeneous opt2 = homogeneous

		attribute = request.json.get('attribute')

		task = roleAllocation.delay(contents = contents, tasks_count = tasks_count,  min_stu_count = min_stu_count, opt = opt, attribute = attribute) #server starts the task, and stores the return value
		print 'task id:', task.id

		return jsonify({ 'id': task.id }), 202,  {'Location': url_for('taskstatus',task_id = task.id)} 
   

@celery.task()
def roleAllocation(contents, tasks_count,  min_stu_count, opt, attribute):

	d = contents['students_data']

	attr_info = {i["id"]:i[attribute] for i in d} 
	new_stu_id = attr_info.keys()
	attr_data = attr_info.values()

	print 'Preprocessing...'

	cost = defaultdict(int)
	if opt == 1:
		for k1,k2 in combinations(attr_info,2):
			if attr_info[k1] == attr_info[k2]:
				cost[(k1,k2)] += 1 #heterogeneity
	if opt == 2:
		for k1, k2 in combinations(attr_info,2):
			if attr_info[k1] != attr_info[k2]:
				cost[(k1,k2)] += 1 #homogeniety

	m = gurobipy.Model("expertmodel")  # definition - gurobi model
	talloc = {}

	for s in new_stu_id:
		for t in tasks_count:
			talloc[s,t] = m.addVar(vtype = gurobipy.GRB.BINARY, name = "talloc") #decision variable

	m.update()
	obj_intermediate = 0

	for s in new_stu_id:
		for j in new_stu_id:
			for k in tasks_count:
				obj_intermediate += talloc[s,k] * talloc[j,k] * cost[s,j]
	m.setObjective(obj_intermediate, gurobipy.GRB.MINIMIZE)

	for s in new_stu_id:                                                     
		m.addConstr(gurobipy.quicksum(talloc[s,t] for t in tasks_count) == 1) #constraint 1
	for t in tasks_count:
		m.addConstr(gurobipy.quicksum(talloc[s,t] for s in new_stu_id) >= min_stu_count) # constraint 2
	m.optimize() #optimize model

	conn = mysql.connect()
	cursor = conn.cursor()
	query1 = "DROP TABLE IF EXISTS task_allocation"
	try:
		cursor.execute(query1)
	except (MySQLdb.Error, MySQLdb.Warning) as e:
		print(e)
		return None

	query2 = "create table task_allocation (stu_id VARCHAR(50), tasks_id INT)" 
	try:
		cursor.execute (query2)
	except (MySQLdb.Error, MySQLdb.Warning) as e:
		print(e)
		return None

	if m.status == gurobipy.GRB.status.OPTIMAL:
		print ('Optimal Task Allocations')
		for s in new_stu_id:
			for t in tasks_count:
				if talloc[s,t].x == 1.0:
					print (s,t)
					try:
						cursor.execute("INSERT INTO task_allocation VALUES (%s,%s)",[s,t])
					except (MySQLdb.Error, MySQLdb.Warning) as e:
						print(e)
						return None

		
		conn.commit()
		conn.close()

	con2 = mysql.connect()
	cursor2 = con2.cursor()
	result3 = cursor2.execute("select stu_id, tasks_id  from task_allocation order by tasks_id")
	data = cursor2.fetchall()

	v={}

	for key, value in data:
		v.setdefault(value, []).append(key)
	print 'results :', v
	con2.close()

	return json.dumps({'role':v},separators = (',', ': ')) #return task allocation results


@app.route('/status/<task_id>') #obtain algorithm processed results
def taskstatus(task_id):
    task = roleAllocation.AsyncResult(task_id)
    response = {'state': task.state, 'status': str(task.info)}
    return jsonify(response)

if __name__ == "__main__": #check if the executed file is the main program and run the app:
	app.run()
