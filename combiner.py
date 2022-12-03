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

            # write the csv headers through the program output
            df = pd.DataFrame(columns = ['email_hash', 'category', 'filename'])
            print(df.to_csv(index = False))

            for filePath in fileList : 
                #get the file name from file path
                fileName = os.path.basename(filePath)

                #since the files can be > 2GB, reading in chunks to prevent memory overflow
                #could use Dask package here but not sure if the files are really that many/that huge to make parrellelism worth it(in smaller scale parrellelism costs performance)
                for chunk in pd.read_csv(filePath, chunksize = 10 ** 6) :
                    
                    #removing the unnecessory columns other than "email_hash" and "category"
                    chunk = chunk.drop(columns = (colname for colname in chunk.columns if 'email_hash' != colname and  'category' != colname))
                    #adding the file name column and write the chunk into the csv file through the program output
                    chunk['filename'] = fileName
                    print(chunk.to_csv(index = False, header = False))

            return
        
        return


def main():
    combiner = Combiner()
    combiner.combineCSV(sys.argv)

if __name__ == '__main__' :
    main()

