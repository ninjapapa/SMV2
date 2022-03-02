import smv

class Employment(smv.SmvCsvFile):
    def path(self):
        return "employment/CB1200CZ11.csv"

    def csvReaderMode(self):
        return "DROPMALFORMED"
