//
// Package:     CondCore/ESSources
// Module:      PoolDBESSource
//
// Description: <one line class summary>
//
// Implementation:
//     <Notes on implementation>
//
// Author:      Zhen Xie
//
// system include files
#include "boost/shared_ptr.hpp"
#include "CondCore/ESSources/interface/PoolDBESSource.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/Connection.h"
#include "CondFormats/Common/interface/Time.h"
#include "CondCore/DBCommon/interface/ConfigSessionFromParameterSet.h"
#include "CondCore/DBCommon/interface/SessionConfiguration.h"
#include "CondCore/DBCommon/interface/PoolTransaction.h"
#include "CondCore/DBCommon/interface/CoralTransaction.h"
#include "CondCore/DBCommon/interface/ConnectionHandler.h"
// #include "FWCore/Framework/interface/DataProxy.h"
#include "CondCore/PluginSystem/interface/DataProxy.h"
#include "CondCore/PluginSystem/interface/ProxyFactory.h"
#include "CondCore/IOVService/interface/PayloadProxy.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
//#include <cstdlib>
#include "TagCollectionRetriever.h"
#include <exception>
//#include <iostream>



namespace {
  std::string
  buildName( const std::string& iRecordName) {
    return iRecordName+"@NewProxy";
  }


  class CondGetterFromESSource : public cond::CondGetter {
  public:
    CondGetterFromESSource(PoolDBESSource::ProxyMap const & ip) : m_proxies(ip){}
    virtual ~CondGetterFromESSource(){}

    cond::IOVProxy get(std::string name) const {
      PoolDBESSource::ProxyMap::const_iterator p = m_proxies.find(name);
      if ( p != m_proxies.end())
	return (*p).second->proxy()->iov();
      return cond::IOVProxy();
    }

    PoolDBESSource::ProxyMap const & m_proxies;
  };


}



PoolDBESSource::PoolDBESSource( const edm::ParameterSet& iConfig ) :
  m_session(), 
  lastRun(0),  // for the refresh
  doRefresh(iConfig.getUntrackedParameter<bool>("RefreshEachRun",false))
{
  //stats = {0,0,0,0,0};
  //std::cout<<"PoolDBESSource::PoolDBESSource"<<std::endl;
  /*parameter set parsing and pool environment setting
   */
  std::string blobstreamerName("");
  if( iConfig.exists("BlobStreamerName") ){
    blobstreamerName=iConfig.getUntrackedParameter<std::string>("BlobStreamerName");
    blobstreamerName.insert(0,"COND/Services/");
    m_session.configuration().setBlobStreamer(blobstreamerName);
  }

 
  std::string userconnect;
  userconnect=iConfig.getParameter<std::string>("connect");
  edm::ParameterSet connectionPset = iConfig.getParameter<edm::ParameterSet>("DBParameters"); 
  cond::ConfigSessionFromParameterSet configConnection(m_session,connectionPset);
  m_session.open();
  
  if(!userconnect.empty())
    cond::ConnectionHandler::Instance().registerConnection(userconnect,m_session,0);
  


  std::string globaltag;
  if( iConfig.exists("globaltag")) globaltag=iConfig.getParameter<std::string>("globaltag");

    cond::Connection* c=cond::ConnectionHandler::Instance().getConnection(userconnect);
    cond::ConnectionHandler::Instance().connect(&m_session);
    cond::CoralTransaction& coraldb=c->coralTransaction();
    
    std::map<std::string,cond::TagMetadata> replacement;
    if( iConfig.exists("toGet") ){
      typedef std::vector< edm::ParameterSet > Parameters;
      Parameters toGet = iConfig.getParameter<Parameters>("toGet");
      for(Parameters::iterator itToGet = toGet.begin(); itToGet != toGet.end(); ++itToGet ) {
	cond::TagMetadata nm;
	nm.recordname=itToGet->getParameter<std::string>("record");
	nm.labelname=itToGet->getUntrackedParameter<std::string>("label","");
	nm.tag=itToGet->getParameter<std::string>("tag");
	nm.pfn=itToGet->getUntrackedParameter<std::string>("connect",userconnect);
	//	nm.objectname=itFound->second;
	std::string k=nm.recordname+"@"+nm.labelname;
	replacement.insert(std::make_pair<std::string,cond::TagMetadata>(k,nm));
      }
    }

    coraldb.start(true);
    fillTagCollectionFromDB(coraldb, globaltag,replacement);
    coraldb.commit();

    TagCollection::iterator it;
    TagCollection::iterator itBeg=m_tagCollection.begin();
    TagCollection::iterator itEnd=m_tagCollection.end();
    for(it=itBeg; it!=itEnd; ++it){
      cond::ConnectionHandler::Instance().registerConnection(it->pfn,m_session,0);
    }

    cond::ConnectionHandler::Instance().connect(&m_session);
    for(it=itBeg;it!=itEnd;++it){
      
      cond::Connection &  conn = *cond::ConnectionHandler::Instance().getConnection(it->pfn);
      cond::CoralTransaction& coraldb=conn.coralTransaction();
      cond::MetaData metadata(coraldb);
      coraldb.start(true);
      cond::MetaDataEntry result;
      metadata.getEntryByTag(it->tag,result);
      coraldb.commit();
      
      
      cond::DataProxyWrapperBase * pb =  cond::ProxyFactory::get()->create(buildName(it->recordname), conn, 
									   cond::DataProxyWrapperBase::Args(result.iovtoken, it->labelname));
      
      ProxyP proxy(pb);
      m_proxies[it->recordname] = proxy;

    }
    
    CondGetterFromESSource visitor(m_proxies);
    ProxyMap::iterator b= m_proxies.begin();
    ProxyMap::iterator e= m_proxies.end();
    for (;b!=e;b++) {

      (*b).second->proxy()->loadMore(visitor);

      edm::eventsetup::EventSetupRecordKey recordKey(edm::eventsetup::EventSetupRecordKey::TypeTag::findType( (*b).first ) );
      if( recordKey.type() != edm::eventsetup::EventSetupRecordKey::TypeTag() ) {
	findingRecordWithKey( recordKey );
	usingRecordWithKey( recordKey );   
      }

    }
    stats.nData=m_proxies.size();
}


PoolDBESSource::~PoolDBESSource() {

  std::cout << "PoolDBESSource Statistics" << std::endl
	    << "Records " << stats.nData
	    <<" setInterval " << stats.nSet
	    <<" Runs " << stats.nRun
	    <<" Refresh " << stats.nRefresh
	    <<" Actual Refresh " << stats.nActualRefresh;
  std::cout << std::endl;
}


//
// member functions
//
void 
PoolDBESSource::setIntervalFor( const edm::eventsetup::EventSetupRecordKey& iKey, const edm::IOVSyncValue& iTime, edm::ValidityInterval& oInterval ){

  stats.nSet++;

  std::string recordname=iKey.name();
  oInterval = edm::ValidityInterval::invalidInterval();
  
  ProxyMap::const_iterator p = m_proxies.find(recordname);
  if ( p == m_proxies.end()) {
    LogDebug ("PoolDBESSource")<<"no DataProxy (Pluging) found for record "<<recordname;
    return;
  }

  {
    // the refresh: each new run: up to us to detect it...
    if(iTime.eventID().run()!=lastRun) {
      lastRun=iTime.eventID().run();
      stats.nRun++;
      if (doRefresh)  { 
	stats.nActualRefresh += (*p).second->proxy()->refresh(); 
	stats.nRefresh++;
      }
    }
  }

  bool userTime=false;
  cond::TimeType timetype = (*p).second->proxy()->timetype();

  cond::Time_t abtime;
  if( timetype == cond::timestamp ){
    abtime=(cond::Time_t)iTime.time().value();
  }else if( timetype == cond::runnumber){
    abtime=(cond::Time_t)iTime.eventID().run();
  }else if( timetype ==  cond::lumiid ){
    edm::LuminosityBlockID lum(iTime.eventID().run(), iTime.luminosityBlockNumber());
    abtime=(cond::Time_t)lum.value();
  }else{
    userTime=true;
  }
  //std::cout<<"abtime "<<abtime<<std::endl;

  if (!userTime) {
    
    cond::ValidityInterval validity = (*p).second->proxy()->setIntervalFor(abtime);
    
    edm::IOVSyncValue start,stop;
    
    if( timetype == cond::timestamp ){
      start=edm::IOVSyncValue( edm::Timestamp(validity.first) );
      stop=edm::IOVSyncValue( edm::Timestamp(validity.second) );
    }else if( timetype == cond::runnumber ){
      start=edm::IOVSyncValue( edm::EventID(validity.first,0) );
      stop=edm::IOVSyncValue( edm::EventID(validity.second,edm::EventID::maxEventNumber()) );
    }else if( timetype == cond::lumiid ){
      edm::LuminosityBlockID lumstart((boost::uint64_t)validity.first);
      start=edm::IOVSyncValue(edm::EventID(lumstart.run(),0), lumstart.luminosityBlock());
      edm::LuminosityBlockID lumstop((boost::uint64_t)validity.second);
      stop=edm::IOVSyncValue(edm::EventID(lumstop.run(),edm::EventID::maxEventNumber()), lumstop.luminosityBlock());
    }
    
    //std::cout<<"setting validity "<<validity.first<<" "<<validity.second<<" for ibtime "<<abtime<< std::endl;
    
    // to force refresh we leave the interval open, so we will get call back at each event...
    oInterval = edm::ValidityInterval( start, doRefresh ?  edm::IOVSyncValue::invalidIOVSyncValue() : stop );
    
  }
  
}

void 
PoolDBESSource::registerProxies(const edm::eventsetup::EventSetupRecordKey& iRecordKey , KeyedProxies& aProxyList) {
  std::string recordname=iRecordKey.name();

  ProxyMap:: const_iterator p = m_proxies.find(recordname);
  if ( p == m_proxies.end()) {
    LogDebug ("PoolDBESSource")<<"no DataProxy (Plugin) found for record "<<recordname;
    return;
  }

  if(0 != (*p).second.get()) {
    edm::eventsetup::TypeTag type =  (*p).second->type(); 
    edm::eventsetup::DataKey key( type, edm::eventsetup::IdTags((*p).second->label().c_str()) );
    aProxyList.push_back(KeyedProxies::value_type(key,(*p).second->edmProxy()));
  }
}

void 
PoolDBESSource::newInterval(const edm::eventsetup::EventSetupRecordKey& iRecordType,const edm::ValidityInterval& iInterval) 
{
  //LogDebug ("PoolDBESSource")<<"newInterval";
  invalidateProxies(iRecordType);
}



void 
PoolDBESSource::fillTagCollectionFromDB( cond::CoralTransaction& coraldb, 
					 const std::string& roottag,
					 std::map<std::string,cond::TagMetadata>& replacement){
  //  std::cout<<"fillTagCollectionFromDB"<<std::endl;
  std::set< cond::TagMetadata > tagcoll;
  if (!roottag.empty()) {
  cond::TagCollectionRetriever tagRetriever( coraldb );
  tagRetriever.getTagCollection(roottag,tagcoll);
  } 

  std::set<cond::TagMetadata>::iterator it;
  std::set<cond::TagMetadata>::iterator itBeg=tagcoll.begin();
  std::set<cond::TagMetadata>::iterator itEnd=tagcoll.end();

  for(it=itBeg; it!=itEnd; ++it){
    std::string k=it->recordname+"@"+it->labelname;
    std::map<std::string,cond::TagMetadata>::iterator fid=replacement.find(k);
    if(fid != replacement.end()){
      cond::TagMetadata m;
      m.recordname=it->recordname;
      m.labelname=it->labelname;
      m.pfn=fid->second.pfn;
      m.tag=fid->second.tag;
      m.objectname=it->objectname;
      m_tagCollection.insert(m);
      replacement.erase(fid);
    }else{
      m_tagCollection.insert(*it);
    }
  }
  std::map<std::string,cond::TagMetadata>::iterator itrep;
  std::map<std::string,cond::TagMetadata>::iterator itrepBeg=replacement.begin();
  std::map<std::string,cond::TagMetadata>::iterator itrepEnd=replacement.end();
  for(itrep=itrepBeg; itrep!=itrepEnd; ++itrep){
    //std::cout<<"appending"<<std::endl;
    //std::cout<<"pfn "<<itrep->second.pfn<<std::endl;
    //std::cout<<"objectname "<<itrep->second.objectname<<std::endl;
    //std::cout<<"tag "<<itrep->second.tag<<std::endl;
    //std::cout<<"recordname "<<itrep->second.recordname<<std::endl;
    m_tagCollection.insert(itrep->second);
  }
}
