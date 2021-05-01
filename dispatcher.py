from flask import Flask, abort, request, jsonify, Response, redirect, url_for
import json
import sched
import time
from datetime import datetime, date, timedelta
import threading

class DispacherUtils ():
    
    def __init__(self):
        self.schedules = None
        self.data = None
        self.schedules_sent = []
        self.scheduler = sched.scheduler(time.time, time.sleep)


    def print_mission(self, programacion, start):
        id_programacion = programacion.get('id')
        mission = programacion.get('mision')
        drone = programacion.get('drone')
        priority = programacion.get('prioridad')
        now = time.time()
        elapsed = int(now - start)
        print('PROGRAMACION # {} : {} elapsed={} mission={} drone={} prioridad={}'. format(id_programacion, time.ctime(now), elapsed, mission, drone, priority))


    def data_is_valid(self):
        return True
 
    
    def start_update(self):
        #RESETEA EL LAS MISIONES PROGRAMADAS EN EL SCHEDULER
        for event in self.scheduler.queue:
            try:
                self.scheduler.cancel(event)
            except ValueError:
                pass


        start = time.time()
        print('START:', time.ctime(start))
        
        now = time.time()
        fecha_actual = datetime.today().strftime("%Y-%m-%d")
        hora_actual = datetime.today().strftime("%H:%M")

        if not self.schedules == None:    
            for schedule in self.schedules.values():
                id_programacion = schedule.get('id')
                fecha_inicial = schedule.get('fecha_inicial')
                fecha_final = schedule.get('fecha_final')
                hora_inicial = schedule.get('hora_inicial')
                hora_final = schedule.get('hora_final')
                prioridad = schedule.get('prioridad')
                print("<<<<<<<<<<<<<<<<<<<<<<PROGRAMACION # %s >>>>>>>>>>>>>>>>>>>>>>" % id_programacion)

                #FECHA ACTUAL Y HORA ACTUAL <<<DENTRO>>> DEL INTERVALO DE LA PROGRAMACION
                if (fecha_inicial <= fecha_actual <= fecha_final) & (hora_inicial <= hora_actual <= hora_final):
                    self.scheduler.enterabs(now, prioridad, self.print_mission, (schedule, start))

                #FECHA ACTUAL <<<DENTRO>>> DEL INTERVALO Y HORA ACTUAL <<<DESPUES>>> DEL INTERVALO DE LA PROGRAMACION
                elif (fecha_inicial <= fecha_actual <= fecha_final) & (hora_actual > hora_final):
                    today = date.today() 
                    tomorrow = today + timedelta(days=1)
                    fecha_final = datetime.strptime(fecha_final, '%Y-%m-%d').date()
                    if fecha_final < tomorrow:
                        print("LA PROGRAMACIÓN {} EXPIRÓ:". format(id_programacion))
                    else:
                        future_date_str = str(tomorrow) + " " + hora_inicial + ":00"
                        future_date = datetime.strptime(future_date_str, '%Y-%m-%d %H:%M:%S')
                        print(future_date)
                        future_date_in_seconds = future_date.timestamp()
                        delay = future_date_in_seconds - start
                        print("DELAY DE LA PROGRAMACION {} : {}". format(id_programacion, delay))
                        self.scheduler.enterabs(now+delay, prioridad, self.print_mission, (schedule, start))

                #FECHA ACTUAL <<<DENTRO>>> DEL INTERVALO Y HORA ACTUAL <<<ANTES>>> DEL INTERVALO DE LA PROGRAMACION
                elif (fecha_inicial <= fecha_actual <= fecha_final) & (hora_actual < hora_final):
                    today = date.today()
                    future_date_str = str(today) + " " + hora_inicial + ":00"
                    future_date = datetime.strptime(future_date_str, '%Y-%m-%d %H:%M:%S')
                    print(future_date)
                    future_date_in_seconds = future_date.timestamp()
                    delay = future_date_in_seconds - start
                    print("DELAY DE LA PROGRAMACION {} : {}". format(id_programacion, delay))
                    self.scheduler.enterabs(now+delay, prioridad, self.print_mission, (schedule, start))

                #FECHA ACTUAL <<<ANTES>>> DEL INTERVALO DE LA PROGRAMACION
                elif (fecha_actual < fecha_inicial):
                    future_date_str = fecha_inicial + " " + hora_inicial + ":00"
                    future_date = datetime. strptime(future_date_str, '%Y-%m-%d %H:%M:%S')
                    print(future_date)
                    future_date_in_seconds = future_date.timestamp()
                    delay = future_date_in_seconds - start
                    print("DELAY DE LA PROGRAMACION {} : {}". format(id_programacion, delay))
                    self.scheduler.enterabs(now+delay, prioridad, self.print_mission, (schedule, start))

                #FECHA ACTUAL <<<DESPUES>>> DEL INTERVALO DE LA PROGRAMACION
                elif (fecha_actual > fecha_final):
                    print("LA PROGRAMACIÓN {} EXPIRÓ:". format(id_programacion))

        else:
            print("NO SCHEDULED MISSIONS")
        
        print("SCHEDULER UPDATED")

        
    def next_mission(self):
        self.start_update()
        next_mission = False
        fecha_actual = datetime.today().strftime("%Y-%m-%d")
        hora_actual = datetime.today().strftime("%H:%M")
        start = time.time()
        now = time.time()

        if not self.scheduler.empty():

            drone = self.data.get('drone')
            for event in self.scheduler.queue:
                schedule = event.argument[0]
                
                new_drone = schedule.get('drone')
                fecha_inicial = schedule.get('fecha_inicial')
                fecha_final = schedule.get('fecha_final')
                hora_inicial = schedule.get('hora_inicial')
                hora_final = schedule.get('hora_final')
                prioridad = schedule.get('prioridad')
                #print(self.scheduler.queue)
                if (not next_mission) & (drone == new_drone) & (fecha_inicial <= fecha_actual <= fecha_final) & (hora_inicial <= hora_actual <= hora_final):
                    next_mission = True
                    print("MISSION # %s IS THE NEXT  MISSION FOR DRON # %s" % (schedule.get('mision'), drone))
                    for event in self.scheduler.queue:
                        try:
                            self.scheduler.cancel(event)
                        except ValueError:
                            pass
                    next_mission = self.scheduler.enterabs(now, prioridad, self.print_mission, (schedule, start))
                    break
                else:
                    pass
                
            
            if next_mission:
                self.scheduler.run()
            else:
                print("NO SCHEDULED MISSIONS FOR DRONE %s" % drone)
        else : 
            print("NO SCHEDULED MISSIONS")

        
dispacher_utils = DispacherUtils()

app = Flask(__name__)

@app.route('/update', methods=["POST"])
def update():
    if request.method=="POST":
        
        dispacher_utils.schedules = request.get_json()
        
        if  dispacher_utils.data_is_valid():
            print ('################## ACTUALIZANDO ################')
            dispacher_utils.start_update()
            scheduler_thread = threading.Thread(name='scheduler', target=dispacher_utils.scheduler.run)
            scheduler_thread.start()
            
            status_code = Response(status=200)
            return status_code
        else:
            status_code = Response(status=400)
            return status_code
    else: 
        abort(405, description="Only allowed POST request")

@app.route('/next_mission', methods=["GET"])
def next_mission():
    if request.method=="GET":

        dispacher_utils.data = request.get_json()

        if dispacher_utils.data_is_valid():
            
            print ('################## RECALCULANDO ################')
            dispacher_utils.next_mission()
            #mission_thread = threading.Thread(name='next_mission', target=dispacher_utils.next_mission)
            #mission_thread.start()

            status_code = Response(status=200)

            return status_code
        else:
            status_code = Response(status=400)
            return status_code
    
    abort(405, description="Only allowed POST request")

app.run()