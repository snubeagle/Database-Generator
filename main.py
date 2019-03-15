#! /usr/bin/python

import MySQLdb, datetime, string, os, random
import user_create, resource, ppitv2, patient_autocreatev2, insurance, incidentv2, fee, appointmentv5, ledger_data_charges, ledger_payments

if __name__ == "__main__":
#    user_create.user()
#    resource.resource_ins()
#    patient_autocreatev2.let_it_begin()
#    fee.run()
#    appointmentv5.work()
#    incidentv2.run()
#    insurance.run_ins()
#    ppitv2.run_ppit()
#    ledger_data_charges.run()
    x = int(raw_input("How many payments would you like to add? "))
    for i in range(x):
        ledger_payments.run()