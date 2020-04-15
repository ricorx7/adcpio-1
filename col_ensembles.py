from pymongo import MongoClient
from bson import ObjectId
from rti_python import Ensemble
import datetime

class ColEnsembles:

    def __init__(self, client: MongoClient):
        # Set the Mongo connection
        self.mongo_client = client

    def write_ens(self, ens: Ensemble.Ensemble, project_id: ObjectId, adcp_id: ObjectId):
        # Get the adcp.io database
        db = self.mongo_client.adcp_io

        # Convert the ensemble to bson
        mongo_ens = self.ens_to_mongo(ens, project_id, adcp_id)
        mongo_amp = self.ens_amp_to_mongo(ens, project_id, adcp_id)

        # Write the single ensemble to the database
        result = db.ensembles.insert_one(mongo_ens)
        result_amp = db.ampltudes.insert_one(mongo_amp)

        return result, result_amp

    def ens_to_mongo(self, ens: Ensemble.Ensemble, project_id: ObjectId, adcp_id: ObjectId, ):

        # Date and time
        dt = datetime.datetime.now()
        ens_num = 0
        ss_index = 0
        ss_config = ''
        if ens.IsEnsembleData:
            dt = ens.EnsembleData.datetime()
            ens_num = ens.EnsembleData.EnsembleNumber
            ss_config = ens.EnsembleData.SubsystemConfig
            ss_index = ens.EnsembleData.SysFirmwareSubsystemCode = ""

        bin_size = 0.0
        if ens.IsAncillaryData:
            bin_size = ens.AncillaryData.BinSize

        # Velocity data
        beam_vel = []
        inst_vel = []
        earth_vel = []
        mag = []
        dir = []
        if ens.IsBeamVelocity:
            beam_vel = ens.BeamVelocity.Velocities
        if ens.IsInstrumentVelocity:
            inst_vel = ens.InstrumentVelocity.Velocities
        if ens.IsEarthVelocity:
            earth_vel = ens.EarthVelocity.Velocities
            mag = ens.EarthVelocity.Magnitude
            dir = ens.EarthVelocity.Direction

        # Amplitude and Correlation
        amp = []
        corr = []
        if ens.IsAmplitude:
            amp = ens.Amplitude.Amplitude
        if ens.IsCorrelation:
            corr = ens.Correlation.Correlation

        # Convert the ensemble to mongodb bson format
        mongo_ens = {
            'adcp_id': adcp_id,
            'prj_id': project_id,
            'dt': dt,
            'ens_num': ens_num,
            'bin_size': bin_size,
            'ss_index': ss_index,
            'ss_config': ss_config,
            'beam_vel': beam_vel,
            'instr_vel': inst_vel,
            'earth_vel': earth_vel,
            'mag': mag,
            "dir": dir,
            "amp": amp,
            "corr": corr
        }

        # return the bson data
        return mongo_ens

    def ens_amp_to_mongo(self, ens: Ensemble.Ensemble, project_id: ObjectId, adcp_id: ObjectId, ):

        # Date and time
        dt = datetime.datetime.now()
        ens_num = 0
        ss_index = 0
        ss_config = ''
        if ens.IsEnsembleData:
            dt = ens.EnsembleData.datetime()
            ens_num = ens.EnsembleData.EnsembleNumber
            ss_config = ens.EnsembleData.SubsystemConfig
            ss_index = ens.EnsembleData.SysFirmwareSubsystemCode = ""

        bin_size = 0.0
        if ens.IsAncillaryData:
            bin_size = ens.AncillaryData.BinSize

        # Amplitude and Correlation
        amp = []
        if ens.IsAmplitude:
            amp = ens.Amplitude.Amplitude

        # Convert the ensemble to mongodb bson format
        mongo_ens = {
            'adcp_id': adcp_id,
            'prj_id': project_id,
            'dt': dt,
            'ens_num': ens_num,
            'bin_size': bin_size,
            'ss_index': ss_index,
            'ss_config': ss_config,
            "amp": amp,
        }

        return mongo_ens