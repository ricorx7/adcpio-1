from pymongo import MongoClient
from rti_python.Utilities.read_binary_file import ReadBinaryFile
from col_ensembles import ColEnsembles
from bson import ObjectId

class ImportFile:

    def __init__(self, mongo_ip: str, mongo_port=27017):
        # Set the IP and Port for the mongo databse
        self.mongo_ip = mongo_ip
        self.mongo_port = mongo_port

        # Create a client
        self.client = MongoClient(mongo_ip, port=mongo_port)

        self.db_ens = ColEnsembles(self.client)
        self.project_id = ObjectId()
        self.adcp_id = ObjectId()


    def import_file(self, file_path: str):
        # Read in the file and add to the database
        # Create the file reader to read the binary file
        read_binary = ReadBinaryFile()
        read_binary.ensemble_event += self.process_ens_func

        # Pass the file path to the reader
        read_binary.playback(file_path)

    def process_ens_func(self, sender, ens):
        """
        Receive the data from the file.  It will process the file.
        When an ensemble is found, it will call this function with the
        complete ensemble.
        :param ens: Ensemble to process.
        :return:
        """
        if ens.IsEnsembleData:
            print(str(ens.EnsembleData.EnsembleNumber))

        # Write the data to the database
        result = self.db_ens.write_ens(ens, self.project_id, self.adcp_id)
        print(result)


if __name__ == '__main__':
    import_file = ImportFile("192.168.1.217", 27017)

    import_file.import_file("G:\\rti\\data\\Lake\\01200000000000000000000000000866\\RTI_20191212125655_00866.bin")