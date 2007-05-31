// system include files
#include "boost/shared_ptr.hpp"
// user include files
#include "CondCore/ESSources/interface/PoolDBESSource.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "CondCore/DBCommon/interface/Time.h"
#include "CondCore/DBCommon/interface/RelationalStorageManager.h"
#include "CondCore/DBCommon/interface/PoolStorageManager.h"
#include "CondCore/DBCommon/interface/ConfigSessionFromParameterSet.h"
#include "CondCore/DBCommon/interface/SessionConfiguration.h"
#include "CondCore/DBCommon/src/ServiceLoader.h"
#include "FWCore/Framework/interface/SourceFactory.h"
#include "FWCore/Framework/interface/DataProxy.h"
#include "CondCore/PluginSystem/interface/ProxyFactory.h"
#include "CondCore/IOVService/interface/IOVService.h"
#include "CondCore/IOVService/interface/IOVNames.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "POOLCore/Exception.h"
#include "RelationalAccess/IConnectionService.h"
#include "RelationalAccess/IWebCacheControl.h"
#include "FWCore/Catalog/interface/SiteLocalConfig.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include <exception>
//#include <iostream>
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include <sstream>
#include <cstdlib>
//
// static data member definitions
//

static
std::string
buildName( const std::string& iRecordName, const std::string& iTypeName ) {
  return iRecordName+"@"+iTypeName+"@Proxy";
}

static
std::pair<std::string,std::string>
deconstructName(const std::string& iProxyName) {
  if(iProxyName.find_first_of("@")==std::string::npos){
    return std::make_pair("","");
  }
  std::string recordName(iProxyName, 0, iProxyName.find_first_of("@"));
  std::string typeName(iProxyName,recordName.size()+1,iProxyName.size()-6-recordName.size()-1);
  //std::cout <<"Record \""<<recordName<<"\" type \""<<typeName<<"\""<<std::endl;
  return std::make_pair(recordName,typeName);
}

#include "FWCore/PluginManager/interface/PluginManager.h"

static
void
fillRecordToTypeMap(std::multimap<std::string, std::string>& oToFill){
  //From the plugin manager get the list of our plugins
  // then from the plugin names, we can deduce the 'record to type' information
  //std::cout<<"Entering fillRecordToTypeMap "<<std::endl;
   
   
   edmplugin::PluginManager*db =  edmplugin::PluginManager::get();
   
   typedef edmplugin::PluginManager::CategoryToInfos CatToInfos;
   
   const std::string mycat(cond::pluginCategory());
   CatToInfos::const_iterator itFound = db->categoryToInfos().find(mycat);
   
   if(itFound == db->categoryToInfos().end()) {
      return;
   }
   std::string lastClass;
   for (edmplugin::PluginManager::Infos::const_iterator itInfo = itFound->second.begin(),
	   itInfoEnd = itFound->second.end(); 
	itInfo != itInfoEnd; ++itInfo)
   {
      if (lastClass == itInfo->name_) {
         continue;
      }
      
      lastClass = itInfo->name_;
      std::pair<std::string,std::string> pairName=deconstructName(lastClass);
      if( pairName.first.empty() ) continue;
      if( oToFill.find(pairName.first)==oToFill.end() ){
	 //oToFill.insert(deconstructName(cap));
	 oToFill.insert(pairName);
      }else{
	 for(std::multimap<std::string, std::string>::iterator pos=oToFill.lower_bound(pairName.first); pos != oToFill.upper_bound(pairName.first); ++pos ){
	    if(pos->second != pairName.second){
	       oToFill.insert(pairName);
	    }else{
	       //std::cout<<"ignore "<<pairName.first<<" "<<pairName.second<<std::endl;
	    }
	 }
      }
   }
}

//
// constructors and destructor
//
PoolDBESSource::PoolDBESSource( const edm::ParameterSet& iConfig ) :
  m_timetype(iConfig.getParameter<std::string>("timetype") ),
  m_session( 0 ), 
  m_connected( false )
{		
  //std::cout<<"PoolDBESSource::PoolDBESSource"<<std::endl;
  /*parameter set parsing and pool environment setting
   */
  std::string catconnect, mycatalog;
  std::string connect;
  connect=iConfig.getParameter<std::string>("connect");
  //catconnect=iConfig.getUntrackedParameter<std::string>("catalog","file::PoolFileCatalog.xml");
  //std::cout<<"catconnect "<<catconnect<<std::endl;
  bool siteLocalConfig=iConfig.getUntrackedParameter<bool>("siteLocalConfig",false);
  m_session=new cond::DBSession(true);
  edm::ParameterSet connectionPset = iConfig.getParameter<edm::ParameterSet>("DBParameters"); 
  //cond::ConfigSessionFromParameterSet configConnection(*m_session,connectionPset);
  //std::cout<<"PoolDBESSource::PoolDBESSource"<<std::endl;
  using namespace edm;
  using namespace edm::eventsetup;  
  fillRecordToTypeMap(m_recordToTypes);
  //parsing record to tag
  std::vector< std::pair<std::string,std::string> > recordToTag;
  typedef std::vector< ParameterSet > Parameters;
  Parameters toGet = iConfig.getParameter<Parameters>("toGet");
  if(0==toGet.size()){
    throw cond::Exception("Configuration") <<" The \"toGet\" parameter is empty, please specify what (Record, tag) pairs you wish to retrieve\n"
					   <<" or use the record name \"all\" to have your tag be used to retrieve all known Records\n";
  }
  if(1==toGet.size() && (toGet[0].getParameter<std::string>("record") =="all") ) {
    //User wants us to read all known records
    // NOTE: In the future, we should only read all known Records for the data that is in the actual database
    //  Can this be done looking at the available IOVs?

    //by forcing this to load, we also load the definition of the Records which 
    //will allow EventSetupRecordKey::TypeTag::findType(...) method to find them
    for(RecordToTypes::iterator itRec = m_recordToTypes.begin();itRec != m_recordToTypes.end();	++itRec ) {
      m_proxyToToken.insert( make_pair(buildName(itRec->first, itRec->second ),"") );
      //fill in dummy tokens now, change in setIntervalFor
      pProxyToToken pos=m_proxyToToken.find(buildName(itRec->first, itRec->second));
      //boost::shared_ptr<DataProxy> proxy(cond::ProxyFactory::get()->create( buildName(itRec->first, itRec->second),m_svc,pos));
      boost::shared_ptr<DataProxy> proxy(cond::ProxyFactory::get()->create( buildName(itRec->first, itRec->second),m_pooldb,pos));
    }
    std::string tagName = toGet[0].getParameter<std::string>("tag");
    //NOTE: should delay setting what  records until all 
    std::string lastRecordName;
    for(RecordToTypes::const_iterator itName = m_recordToTypes.begin();itName != m_recordToTypes.end();++itName) {
      if(lastRecordName != itName->first) {
	lastRecordName = itName->first;
	//std::cout<<"lastRecordName "<<lastRecordName<<std::endl;
	EventSetupRecordKey recordKey = EventSetupRecordKey::TypeTag::findType(itName->first);
	if (recordKey == EventSetupRecordKey()) {
	  LogDebug ("")<< "The Record type named \""<<itName->first<<"\" could not be found.  We therefore assume it is not needed for this job";
	} else {
	  //std::cout<<"finding Record with key"<<std::endl;
	  findingRecordWithKey(recordKey);
	  //std::cout<<"using Record with key"<<std::endl;
	  usingRecordWithKey(recordKey);
	  recordToTag.push_back(std::make_pair(itName->first, tagName));
	}
      }
    }
  } else {
    std::string lastRecordName;
    for(Parameters::iterator itToGet = toGet.begin(); itToGet != toGet.end(); ++itToGet ) {
      std::string recordName = itToGet->getParameter<std::string>("record");
      std::string tagName = itToGet->getParameter<std::string>("tag");     
      //load proxy code now to force in the Record code
      std::multimap<std::string, std::string>::iterator itFound=m_recordToTypes.find(recordName);
      if(itFound == m_recordToTypes.end()){
	throw cond::Exception("NoRecord")<<" The record \""<<recordName<<"\" is not known by the PoolDBESSource";
      }
      std::string typeName = itFound->second;
      std::string proxyName = buildName(recordName,typeName);
      m_proxyToToken.insert( make_pair(proxyName,"") );
      //fill in dummy tokens now, change in setIntervalFor
      pProxyToToken pos=m_proxyToToken.find(proxyName);
      boost::shared_ptr<DataProxy> proxy(cond::ProxyFactory::get()->create(proxyName,m_pooldb,pos));
      eventsetup::EventSetupRecordKey recordKey(eventsetup::EventSetupRecordKey::TypeTag::findType( recordName ) );
      if( recordKey.type() == eventsetup::EventSetupRecordKey::TypeTag() ) {
	//record not found
	throw cond::Exception("NoRecord")<<"The record \""<< recordName <<"\" does not exist ";
      }
      recordToTag.push_back(std::make_pair(recordName, tagName));
      if( lastRecordName != recordName ) {
	lastRecordName = recordName;
	findingRecordWithKey( recordKey );
	usingRecordWithKey( recordKey );
      }
    }
  }
  cond::ConfigSessionFromParameterSet configConnection(*m_session,connectionPset);
  //std::string authpath("CORAL_AUTH_PATH=");
  //authpath+=m_session->sessionConfiguration().authName();
  //::putenv(const_cast<char*>(authpath.c_str()));
  m_session->open();

  if( siteLocalConfig ){
    edm::Service<edm::SiteLocalConfig> localconfservice;
    if( !localconfservice.isAvailable() ){
      throw cms::Exception("edm::SiteLocalConfigService is not available");       
    }
    connect=localconfservice->lookupCalibConnect(connect);
    catconnect=iConfig.getUntrackedParameter<std::string>("catalog","");
    if(catconnect.empty()){
      mycatalog=localconfservice->calibCatalog();
    }else{
      mycatalog=catconnect;
    }
    std::string logicalconnect=localconfservice->calibLogicalServer();
    
    //get handle to IConnectionService
    seal::IHandle<coral::IConnectionService>
      connSvc = m_session->serviceLoader().context()->query<coral::IConnectionService>( "CORAL/Services/ConnectionService" );
    //get handle to webCacheControl()
    connSvc->webCacheControl().refreshTable( logicalconnect,cond::IOVNames::iovTableName() );
    connSvc->webCacheControl().refreshTable( logicalconnect,cond::IOVNames::iovDataTableName() );
  }else{
    mycatalog=iConfig.getUntrackedParameter<std::string>("catalog","");
  }
  m_con=connect;
  m_pooldb=new cond::PoolStorageManager(m_con,mycatalog,m_session);
  if(m_timetype=="timestamp"){
    m_iovservice=new cond::IOVService(*m_pooldb,cond::timestamp);
  }else{
    m_iovservice=new cond::IOVService(*m_pooldb,cond::runnumber);
  }
  this->tagToToken(recordToTag);
}
PoolDBESSource::~PoolDBESSource()
{
  // std::cout<<"PoolDBESSource::~PoolDBESSource"<<std::endl;
  if(m_session->isActive()){
    m_pooldb->disconnect();
    m_session->close();
  }
  delete m_iovservice;
  delete m_pooldb;
  delete m_session;
}
//
// member functions
//
void 
PoolDBESSource::setIntervalFor( const edm::eventsetup::EventSetupRecordKey& iKey, const edm::IOVSyncValue& iTime, edm::ValidityInterval& oInterval ) {
  LogDebug ("PoolDBESSource")<<iKey.name();
  //std::cout<<"PoolDBESSource::setIntervalFor "<< iKey.name() <<" at time "<<iTime.eventID().run()<<std::endl;
  RecordToTypes::iterator itRec = m_recordToTypes.find( iKey.name() );
  if( itRec == m_recordToTypes.end() ) {
    //no valid record
    LogDebug ("PoolDBESSource")<<"no valid record ";
    //std::cout<<"no valid record "<<std::endl;
    oInterval = edm::ValidityInterval::invalidInterval();
    return;
  }
  //std::cout<<"recordToIOV size "<<m_recordToIOV.size()<<std::endl;
  RecordToIOV::iterator itIOV = m_recordToIOV.find( iKey.name() );
  if( itIOV == m_recordToIOV.end() ){
    LogDebug ("PoolDBESSource")<<"no valid IOV found for record "<<iKey.name();
    oInterval = edm::ValidityInterval::invalidInterval();
    return;
  }
  std::string iovToken=itIOV->second;
  std::string payloadToken;
  cond::Time_t abtime;
  std::ostringstream os;
  if( m_timetype == "timestamp" ){
    abtime=(cond::Time_t)iTime.time().value();
  }else{
    abtime=(cond::Time_t)iTime.eventID().run();
  }
  //valid time check
  //check if current run exceeds iov upperbound
  m_pooldb->startTransaction(true);
  if( !m_iovservice->isValid(iovToken,abtime) ){
    os<<abtime;
    throw cond::noDataForRequiredTimeException("PoolDBESSource::setIntervalFor",iKey.name(),os.str());
  }
  std::pair<cond::Time_t, cond::Time_t> validity=m_iovservice->validity(iovToken,abtime);
  payloadToken=m_iovservice->payloadToken(iovToken,abtime);
  m_pooldb->commit();
  edm::IOVSyncValue start;
  if( m_timetype == "timestamp" ){
    start=edm::IOVSyncValue( edm::Timestamp(validity.first) );
  }else{
    start=edm::IOVSyncValue( edm::EventID(validity.first,0) );
  }
  edm::IOVSyncValue stop;
  if( m_timetype == "timestamp" ){
    stop=edm::IOVSyncValue( edm::Timestamp(validity.second) );
    LogDebug ("PoolDBESSource")
      <<" set start time "<<start.time().value()
      <<" ; set stop time "<<stop.time().value();
  }else{
    stop=edm::IOVSyncValue( edm::EventID(validity.second,0) );
    LogDebug ("PoolDBESSource")
      <<" set start run "<<start.eventID().run()
      <<" ; set stop run "<<stop.eventID().run();
  }
  oInterval = edm::ValidityInterval( start, stop );
  //std::cout<<"setting itRec->first "<<itRec->first<<std::endl;
  //std::cout<<"setting itRec->second "<<itRec->second<<std::endl;
  //std::cout<<"payloadToken "<< payloadToken<<std::endl;
  //std::cout<<"buildProxy "<<buildName(itRec->first ,itRec->second)<<std::endl;
  m_proxyToToken[buildName(itRec->first ,itRec->second)]=payloadToken;  
}   

void 
PoolDBESSource::registerProxies(const edm::eventsetup::EventSetupRecordKey& iRecordKey , KeyedProxies& aProxyList) 
{
  //LogDebug ("PoolDBESSource ")<<"registerProxies";
  using namespace edm;
  using namespace edm::eventsetup;
  //using namespace std;
  //std::cout <<"registering Proxies for "<< iRecordKey.name() << std::endl;
  //For each data type in this Record, create the proxy by dynamically loading it
  std::pair< RecordToTypes::iterator,RecordToTypes::iterator > typeItrs = m_recordToTypes.equal_range( iRecordKey.name() );
  //loop over types in the same record
  for( RecordToTypes::iterator itType = typeItrs.first; itType != typeItrs.second; ++itType ) {
    //std::cout<<"Entering loop PoolDBESSource::registerProxies"<<std::endl;
    //std::cout<<std::string("   ") + itType->second <<std::endl;
    static eventsetup::TypeTag defaultType;
    eventsetup::TypeTag type = eventsetup::TypeTag::findType( itType->second );
    if( type != defaultType ) {
      pProxyToToken pos=m_proxyToToken.find(buildName(iRecordKey.name(), type.name()));
      boost::shared_ptr<DataProxy> proxy(cond::ProxyFactory::get()->create( buildName(iRecordKey.name(), type.name() ), m_pooldb, pos));
      if(0 != proxy.get()) {
	eventsetup::DataKey key( type, "");
	aProxyList.push_back(KeyedProxies::value_type(key,proxy));
      }
    }
  }
  if( !m_connected ){
    m_pooldb->connect();
    m_connected=true;
  }
}

void 
PoolDBESSource::newInterval(const edm::eventsetup::EventSetupRecordKey& iRecordType,const edm::ValidityInterval& iInterval) 
{
  //std::cout<<"PoolDBESSource::newInterval"<<std::endl;
  LogDebug ("PoolDBESSource")<<"newInterval";
  invalidateProxies(iRecordType);
}

void PoolDBESSource::tagToToken( const std::vector< std::pair < std::string, std::string> >& recordToTag ){
  //std::cout<<"tag to token"<<std::endl;
  try{
    if( recordToTag.size()==0 ) return;
    cond::RelationalStorageManager coraldb(m_con,m_session);
    cond::MetaData metadata(coraldb);
    coraldb.connect(cond::ReadOnly);
    coraldb.startTransaction(true);
    std::vector< std::pair<std::string, std::string> >::const_iterator it;
    for(it=recordToTag.begin(); it!=recordToTag.end(); ++it){
      std::string iovToken=metadata.getToken(it->second);
      if( iovToken.empty() ){
	throw cond::Exception("PoolDBESSource::tagToToken: tag "+it->second+std::string(" has empty iov token") );
      }
      m_recordToIOV.insert(std::make_pair(it->first,iovToken));
    }
    coraldb.commit();
    coraldb.disconnect();
  }catch(const cond::Exception&e ){
    throw e;
  }catch(const cms::Exception&e ){
    throw e;
  }
}


