import FWCore.ParameterSet.Config as cms
import FWCore.ParameterSet.VarParsing as VarParsing
options = VarParsing.VarParsing()
options.register('runNumber',
                 4294967294, #default value
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.int,
                 "Run number; default gives latest IOV")
options.register('globalTag',
                 'IDEAL', #default value
                 VarParsing.VarParsing.multiplicity.singleton,
                 VarParsing.VarParsing.varType.string,
                 "GlobalTag")
options.parseArguments()


process = cms.Process("TEST")

process.add_(cms.Service("PrintEventSetupDataRetrieval", printProviders=cms.untracked.bool(True)))

process.load("Configuration.StandardSequences.FrontierConditions_GlobalTag_cff")
process.GlobalTag.globaltag = options.globalTag+'::all'
process.GlobalTag.DumpStat =  True
# 'GR09_P_V6::All'
#'CRAFT09_R_V9::All'
#'MC_31X_V9::All'
#'GR09_31X_V5P::All'
#process.GlobalTag.pfnPrefix = "frontier://FrontierArc/"
#process.GlobalTag.pfnPostfix = "_0911"
#process.GlobalTag.toGet = cms.VPSet()
#process.GlobalTag.toGet.append(
#   cms.PSet(record = cms.string("BeamSpotObjectsRcd"),
#            tag = cms.string("firstcollisions"),
#             connect = cms.untracked.string("frontier://PromptProd/CMS_COND_31X_BEAMSPOT")
#           )
#)



process.source = cms.Source("EmptyIOVSource",
    lastValue = cms.uint64(3),
    timetype = cms.string('runnumber'),
    firstValue = cms.uint64(options.runNumber-1),
    interval = cms.uint64(1)
)


process.get = cms.EDFilter("EventSetupRecordDataGetter",
                           verbose = cms.untracked.bool(True)
                           )

process.p = cms.Path(process.get)



