import FWCore.ParameterSet.Config as cms

process = cms.Process("TEST")
process.load("CondCore.DBCommon.CondDBCommon_cfi")
process.CondDBCommon.connect = cms.string('oracle://cms_orcoff_int2r/CMS_COND_PRESH')
process.CondDBCommon.DBParameters.authenticationPath = cms.string('/afs/cern.ch/cms/DB/conddb')
process.CondDBCommon.DBParameters.messageLevel = 1
process.PoolDBESSource = cms.ESSource("PoolDBESSource",
    process.CondDBCommon,
    toGet = cms.VPSet(cms.PSet(
        record = cms.string('PedestalsRcd'),
        tag = cms.string('mytest')
    ))
)

process.source = cms.Source("EmptyIOVSource",
    lastRun = cms.untracked.uint32(10),
    timetype = cms.string('runnumber'),
    firstRun = cms.untracked.uint32(1),
    interval = cms.uint32(1)
)

process.prod = cms.EDAnalyzer("PedestalsAnalyzer")

process.p = cms.Path(process.prod)


