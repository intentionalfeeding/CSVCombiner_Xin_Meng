import pandas as pd
import os
import sys

class Combiner:

    
    @staticmethod
    def checkCommand(argv):

        fileList = argv[1 : ]
        
        # check if there is wrong file type, or the file doesn't exist
        for filePath in fileList:
            if not filePath.endswith('.csv'):
                print("Wrong file format")
                return False
            if not os.path.exists(filePath):
                print("No such file exists :" + filePath)
                return False
        
        return True

    def combineCSV(self, argv : list):

        
        if self.checkCommand(argv) :

            fileList = argv[1 : ]

            #chunks holds blocks of dataframes read from CSVs
            df = pd.DataFrame()
            chunks = []

            for filePath in fileList : 
                #get the file name from file path
                fileName = os.path.basename(filePath)

                #since the files can be > 2GB, reading in chunks to prevent memory overflow
                #could use Dask package here but not sure if the files are really that many/that huge to make parrellelism worth it(in smaller scale parrellelism costs performance)
                for chunk in pd.read_csv(filePath, chunksize = 10 ** 6) :
                    #adding the file name column and append each chunk to chunks
                    chunk['filename'] = fileName
                    chunks.append(chunk)
            
            #combine chunks into a single dataframe
            df = pd.concat(chunks, axis = 0, ignore_index = True)
            #There was an additional index column and another empty column in the end, so I drop them here
            df = df.iloc[:, 1 : -1]
            #write the csv with given name in the command line argument
            print(df.to_csv(index = False))

            return
        
        return


def main():
    combiner = Combiner()
    combiner.combineCSV(sys.argv)

if __name__ == '__main__' :
    main()

    
