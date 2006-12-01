// system include files
#include "boost/shared_ptr.hpp"
// user include files
#include "PoolDBESSource.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "CondCore/DBCommon/interface/Time.h"
#include "CondCore/DBCommon/interface/RelationalStorageManager.h"
#include "CondCore/DBCommon/interface/ConfigSessionFromParameterSet.h"
#include "FWCore/Framework/interface/SourceFactory.h"
#include "FWCore/Framework/interface/DataProxy.h"
#include "CondCore/PluginSystem/interface/ProxyFactory.h"
#include "CondCore/IOVService/interface/IOVService.h"
#include "CondCore/IOVService/interface/IOVIterator.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "POOLCore/Exception.h"
#include "FWCore/Framework/interface/SiteLocalConfig.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include <exception>
#include <iostream>
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include <sstream>
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

#include "PluginManager/PluginManager.h"
#include "PluginManager/ModuleCache.h"
#include "PluginManager/Module.h"

static
void
fillRecordToTypeMap(std::multimap<std::string, std::string>& oToFill){
  //From the plugin manager get the list of our plugins
  // then from the plugin names, we can deduce the 'record to type' information
  //std::cout<<"Entering fillRecordToTypeMap "<<std::endl;
  seal::PluginManager                       *db =  seal::PluginManager::get();
  seal::PluginManager::DirectoryIterator    dir;
  seal::ModuleCache::Iterator               plugin;
  seal::ModuleDescriptor                    *cache;
  unsigned                            i;
            
  const std::string mycat(cond::ProxyFactory::pluginCategory());
      
  for (dir = db->beginDirectories(); dir != db->endDirectories(); ++dir) {
    for (plugin = (*dir)->begin(); plugin != (*dir)->end(); ++plugin) {
      for (cache=(*plugin)->cacheRoot(), i=0; i < cache->children(); ++i) {
	//std::cout <<" "<<cache->child(i)->token(0)<<std::endl;
	if (cache->child(i)->token(0) == mycat) {
	  const std::string cap = cache->child(i)->token(1);
	  std::pair<std::string,std::string> pairName=deconstructName(cap);
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
    }
  }
}

//
// constructors and destructor
//
cond::PoolDBESSource::PoolDBESSource( const edm::ParameterSet& iConfig ) :
  m_timetype(iConfig.getParameter<std::string>("timetype") ),
  m_session( 0 ),
  m_tagTranslated( false )
{		
  /*parameter set parsing and pool environment setting
   */
  std::string catconnect;
  std::string connect;
  connect=iConfig.getParameter<std::string>("connect");
  catconnect=iConfig.getUntrackedParameter<std::string>("catalog","file::PoolFileCatalog.xml");
  bool siteLocalConfig=iConfig.getUntrackedParameter<bool>("siteLocalConfig",false);
  if( siteLocalConfig ){
    edm::Service<edm::SiteLocalConfig> localconfservice;
    if( !localconfservice.isAvailable() ){
      throw cms::Exception("edm::SiteLocalConfigService is not available");       
    }
    connect=localconfservice->lookupCalibConnect(connect);
    catconnect=iConfig.getUntrackedParameter<std::string>("catalog","");
    if(catconnect.empty()){
      m_catalog=localconfservice->calibCatalog();
    }
  }else{
    m_catalog=iConfig.getUntrackedParameter<std::string>("catalog","");
  }
  m_session=new cond::DBSession(connect);
  edm::ParameterSet connectionPset = iConfig.getParameter<edm::ParameterSet>("DBParameters"); 
  ConfigSessionFromParameterSet configConnection(*m_session,connectionPset);
  //std::cout<<"PoolDBESSource::PoolDBESSource"<<std::endl;
  using namespace std;
  using namespace edm;
  using namespace edm::eventsetup;
  fillRecordToTypeMap(m_recordToTypes);
  //parsing record to tag
  //  std::vector< std::pair<std::string,std::string> > recordToTag;
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
      boost::shared_ptr<DataProxy> proxy(cond::ProxyFactory::get()->create( buildName(itRec->first, itRec->second),m_session->poolStorageManager(m_catalog),pos));
    }
    string tagName = toGet[0].getParameter<string>("tag");
    //NOTE: should delay setting what  records until all 
    string lastRecordName;
    for(RecordToTypes::const_iterator itName = m_recordToTypes.begin();itName != m_recordToTypes.end();++itName) {
      if(lastRecordName != itName->first) {
	lastRecordName = itName->first;
	//std::cout<<"lastRecordName "<<lastRecordName<<std::endl;
	EventSetupRecordKey recordKey = EventSetupRecordKey::TypeTag::findType(itName->first);
	if (recordKey == EventSetupRecordKey()) {
	  LogDebug ("")<< "The Record type named \""<<itName->first<<"\" could not be found.  We therefore assume it is not needed for this job";
	} else {
	  findingRecordWithKey(recordKey);
	  usingRecordWithKey(recordKey);
	  m_recordToTag.push_back(std::make_pair(itName->first, tagName));
	}
      }
    }
  } else {
    string lastRecordName;
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
      boost::shared_ptr<DataProxy> proxy(cond::ProxyFactory::get()->create(proxyName,m_session->poolStorageManager(m_catalog),pos));
      eventsetup::EventSetupRecordKey recordKey(eventsetup::EventSetupRecordKey::TypeTag::findType( recordName ) );
      if( recordKey.type() == eventsetup::EventSetupRecordKey::TypeTag() ) {
	//record not found
	throw cond::Exception("NoRecord")<<"The record \""<< recordName <<"\" does not exist ";
      }
      m_recordToTag.push_back(std::make_pair(recordName, tagName));
      if( lastRecordName != recordName ) {
	lastRecordName = recordName;
	findingRecordWithKey( recordKey );
	usingRecordWithKey( recordKey );
      }
    }
  }
}


cond::PoolDBESSource::~PoolDBESSource()
{
  if(m_session->isActive()){
    m_session->close();
  }
  delete m_session;
 }
//
// member functions
//
void 
cond::PoolDBESSource::setIntervalFor( const edm::eventsetup::EventSetupRecordKey& iKey, const edm::IOVSyncValue& iTime, edm::ValidityInterval& oInterval ) {
  LogDebug ("")<<"setIntervalFor "<<iKey.name();
  RecordToTypes::iterator itRec = m_recordToTypes.find( iKey.name() );
  //std::cout<<"setIntervalFor "<<iKey.name()<<std::endl;
  if( itRec == m_recordToTypes.end() ) {
    //no valid record
    LogDebug ("")<<"no valid record ";
    oInterval = edm::ValidityInterval::invalidInterval();
    return;
  }
  if( m_recordToIOV.size()==0 && !m_tagTranslated ){
    this->tagToToken();
  }
  RecordToIOV::iterator itIOV = m_recordToIOV.find( iKey.name() );
  if( itIOV == m_recordToIOV.end() ){
    LogDebug ("")<<"no valid IOV found for record "<<iKey.name();
    oInterval = edm::ValidityInterval::invalidInterval();
    return;
  }
  std::string iovToken=itIOV->second;
  cond::PoolStorageManager& pooldb=m_session->poolStorageManager(m_catalog);
  cond::IOVService iovservice(pooldb);
  cond::IOVIterator* iovIterator= iovservice.newIOVIterator(iovToken);
  pooldb.connect();
  pooldb.startTransaction(true);
  std::string payloadToken;
  cond::Time_t abtime,starttime;
  cond::Time_t beginOftime,endOftime;
  std::ostringstream os;
  if( m_timetype == "timestamp" ){
    abtime=(cond::Time_t)iTime.time().value();
    endOftime=(cond::Time_t)edm::IOVSyncValue::endOfTime().time().value();
  }else{
    abtime=(cond::Time_t)iTime.eventID().run();
    endOftime=(cond::Time_t)edm::IOVSyncValue::endOfTime().eventID().run();
  }
  //valid time check
  //check if current run exceeds iov upperbound
  iovIterator->next();
  if( !iovIterator->isValid(abtime) ){
    os<<abtime;
    throw cond::noDataForRequiredTimeException("PoolDBESSource::setIntervalFor",iKey.name(),os.str());
  }else{
    if( m_timetype == "timestamp" ){
      beginOftime=edm::IOVSyncValue::beginOfTime().time().value();
      starttime=beginOftime;
    }else{
      beginOftime=(unsigned long long)edm::IOVSyncValue::beginOfTime().eventID().run();
      starttime=beginOftime;
    }
    payloadToken=iovIterator->payloadToken();
    std::pair<cond::Time_t, cond::Time_t> validity=iovIterator->validity();
    edm::IOVSyncValue start;
    if( m_timetype == "timestamp" ){
      start=edm::IOVSyncValue( edm::Timestamp(validity.first) );
    }else{
      start=edm::IOVSyncValue( edm::EventID(validity.first,0) );
    }
    edm::IOVSyncValue stop;
    if( m_timetype == "timestamp" ){
      stop=edm::IOVSyncValue( edm::Timestamp(validity.second) );
      LogDebug ("")<<" set start time "<<start.time().value()
      		   <<" ; set stop time "<<stop.time().value();
    }else{
      stop=edm::IOVSyncValue( edm::EventID(validity.second,0) );
      LogDebug ("")<<" set start run "<<start.eventID().run()
      		   <<" ; set stop run "<<stop.eventID().run();
    }
    oInterval = edm::ValidityInterval( start, stop );
  }
  pooldb.commit();
  pooldb.disconnect();
  delete iovIterator;
  m_proxyToToken[buildName(itRec->first ,itRec->second)]=payloadToken;  
}   

void 
cond::PoolDBESSource::registerProxies(const edm::eventsetup::EventSetupRecordKey& iRecordKey , KeyedProxies& aProxyList) 
{
  LogDebug ("")<<"registerProxies";
  using namespace edm;
  using namespace edm::eventsetup;
  using namespace std;
  //cout <<string("registering Proxies for ") + iRecordKey.name() << endl;
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
      // std::cout<<"about to start pool readl only transaction"<<std::endl;
      boost::shared_ptr<DataProxy> proxy(cond::ProxyFactory::get()->create( buildName(iRecordKey.name(), type.name() ), m_session->poolStorageManager(m_catalog), pos));
      if(0 != proxy.get()) {
	eventsetup::DataKey key( type, "");
	aProxyList.push_back(KeyedProxies::value_type(key,proxy));
      }//else{
      // throw cond::Exception("no valid payload found");
      //}
    }else{
       //std::cout<<"IS default type "<<std::endl;
    }
    //cout <<endl;
  }
}

void 
cond::PoolDBESSource::newInterval(const edm::eventsetup::EventSetupRecordKey& iRecordType,const edm::ValidityInterval& iInterval) 
{
  LogDebug ("")<<"newInterval";
  invalidateProxies(iRecordType);
}

void cond::PoolDBESSource::tagToToken(){
  try{
    if( m_recordToTag.size()==0 ) return;
    if( !m_session->isActive() ){
      m_session->open(true);
    }
    cond::RelationalStorageManager& coraldb=m_session->relationalStorageManager();
    cond::MetaData metadata(coraldb);
    coraldb.connect(cond::ReadOnly);
    coraldb.startTransaction(true);
    std::vector< std::pair<std::string, std::string> >::const_iterator it;
    for(it=m_recordToTag.begin(); it!=m_recordToTag.end(); ++it){
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
  m_tagTranslated=true;
}

// ------------ method called to produce the data  ------------

//define this as a plug-in
using namespace cond;
DEFINE_FWK_EVENTSETUP_SOURCE(PoolDBESSource)
  
