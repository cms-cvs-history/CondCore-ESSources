// system include files
#include "boost/shared_ptr.hpp"
// user include files
#include "CondCore/ESSources/interface/PoolDBESSource.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/ServiceLoader.h"
#include "FWCore/Framework/interface/SourceFactory.h"
#include "FWCore/Framework/interface/DataProxy.h"
#include "CondCore/PluginSystem/interface/ProxyFactory.h"
#include "CondCore/IOVService/interface/IOV.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "POOLCore/Exception.h"
#include <exception>
#include <iostream>
#include <sstream>
//
// static data member definitions
//
static
std::string
buildName( const std::string& iRecordName, const std::string& iTypeName ) {
  //std::cout<<"building name "<<iRecordName+"_"+iTypeName+"_Proxy"<<std::endl;
  return iRecordName+"_"+iTypeName+"_Proxy";
}

static
std::pair<std::string,std::string>
deconstructName(const std::string& iProxyName) {
  std::string recordName(iProxyName, 0, iProxyName.find_first_of("_"));
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
      
      
  //std::cout <<"LoadAllDictionaries"<<std::endl;
  
  const std::string mycat(cond::ProxyFactory::pluginCategory());
      
  for (dir = db->beginDirectories(); dir != db->endDirectories(); ++dir) {
    for (plugin = (*dir)->begin(); plugin != (*dir)->end(); ++plugin) {
      for (cache=(*plugin)->cacheRoot(), i=0; i < cache->children(); ++i) {
	//std::cout <<" "<<cache->child(i)->token(0)<<std::endl;
	if (cache->child(i)->token(0) == mycat) {
	  const std::string cap = cache->child(i)->token(1);
	  std::pair<std::string,std::string> pairName=deconstructName(cap);
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

void PoolDBESSource::initPool(const std::string& catcontact){
  m_session->setCatalog(catcontact);
  try{
    m_session->connect( cond::ReadOnly );
    m_session->startReadOnlyTransaction();
  }catch( const cond::Exception& e){
    throw e;
  } catch(...) {
    throw cond::Exception(std::string("PoolDBESSource::initPool unknown error"));
  }
}

void PoolDBESSource::closePool(){
  m_session->commit();
  m_session->disconnect();
}

bool PoolDBESSource::initIOV( const std::vector< std::pair < std::string, std::string> >& recordToTag ){
  //std::cout<<"PoolDBESSource::initIOV"<<std::endl;
  cond::MetaData meta(m_con, *m_loader);
  std::vector< std::pair<std::string, std::string> >::const_iterator it;
  try{
    meta.connect();
    for(it=recordToTag.begin(); it!=recordToTag.end(); ++it){
      std::string iovToken=meta.getToken(it->second);
      if( iovToken.empty() ){
	return false;
      }
      //std::cout<<"initIOV record: "<<it->first<<std::endl;
      //std::cout<<"initIOV tag: "<<it->second<<std::endl;
      //std::cout<<"initIOV iovToken: "<<iovToken<<std::endl;
      pool::Ref<cond::IOV> iov(&(m_session->DataSvc()), iovToken);
      m_recordToIOV.insert(std::make_pair(it->first,iov));
    }
    meta.disconnect();
    //}catch( const coral::RelationalTableNotFound& e ){
    //std::cerr<<"Caught pool::RelationalTableNotFound Exception"<<std::endl;
    //throw cms::Exception( e.what() );
  }catch(const cond::Exception&e ){
    throw e;
  }catch(const cms::Exception&e ){
    throw e;
  }catch(const pool::Exception&e ){
    throw cond::Exception( "PoolDBESSource::initIOV ")<<e.what();
  }catch(const std::exception& e){
    throw cond::Exception( "PoolDBESSource::initIOV ")<<e.what();
  }catch(...){
    throw cond::Exception( "PoolDBESSource::initIOV " );
  }
  //pool::Ref<cond::IOV> iov(m_svc, iovToken);
  //std::pair<int,std::string> iovpair=*iov->iov.lower_bound(7);
  //m_iov=iov;
  return true;
}

//
// constructors and destructor
//
PoolDBESSource::PoolDBESSource( const edm::ParameterSet& iConfig ) :
  m_con(iConfig.getParameter<std::string>("connect") ),
  m_timetype(iConfig.getParameter<std::string>("timetype") ),
  m_loader(new cond::ServiceLoader),
  m_session(new cond::DBSession(m_con))
{		
  /*parameter set parsing and pool environment setting
   */
  //std::cout<<"PoolDBESSource::PoolDBESSource"<<std::endl;
  unsigned int auth=iConfig.getUntrackedParameter<unsigned int>("authenticationMethod",0) ;
  bool loadblob=iConfig.getUntrackedParameter<bool>("loadBlobStreamer",false);
  std::string catconnect;
  try{
    if( auth==1 ){
      m_loader->loadAuthenticationService( cond::XML );
    }else{
      m_loader->loadAuthenticationService( cond::Env );
    }
    catconnect=iConfig.getUntrackedParameter<std::string>("catalog","");
    unsigned int message_level=iConfig.getUntrackedParameter<unsigned int>("messagelevel",0);
    switch (message_level) {
    case 0 :
      m_loader->loadMessageService(cond::Error);
      break;    
    case 1:
      m_loader->loadMessageService(cond::Warning);
      break;
    case 2:
      m_loader->loadMessageService( cond::Info );
      break;
    case 3:
      m_loader->loadMessageService( cond::Debug );
      break;  
    default:
      m_loader->loadMessageService();
    }
    if( loadblob ){
      m_loader->loadBlobStreamingService();
    }
  }catch( const cond::Exception& e){
    throw e;
  }catch(const cms::Exception&e ){
    throw e;
  }catch( const pool::Exception& e){
    throw cond::Exception( "PoolDBESSource::PoolDBESSource ")<<e.what();
  }catch( const std::exception& e){
    throw cond::Exception( "PoolDBESSource::PoolDBESSource ")<<e.what();
  }catch ( ... ) {
    throw cond::Exception("PoolDBESSource::PoolDBESSource unknown error");
  }
  //std::cout<<"PoolDBESSource::PoolDBESSource"<<std::endl;
  using namespace std;
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
      boost::shared_ptr<DataProxy> proxy(cond::ProxyFactory::get()->create( buildName(itRec->first, itRec->second),&(m_session->DataSvc()),pos));
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
	  cout << "The Record type named \""<<itName->first<<"\" could not be found.  We therefore assume it is not needed for this job"
	       << endl;
	} else {
	  findingRecordWithKey(recordKey);
	  usingRecordWithKey(recordKey);
	  recordToTag.push_back(std::make_pair(itName->first, tagName));
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
      //std::cout<<"proxy "<<proxyName<<std::endl;
      m_proxyToToken.insert( make_pair(proxyName,"") );
      //fill in dummy tokens now, change in setIntervalFor
      pProxyToToken pos=m_proxyToToken.find(proxyName);
      boost::shared_ptr<DataProxy> proxy(cond::ProxyFactory::get()->create(proxyName,&(m_session->DataSvc()),pos));
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

  ///
  //now do what ever other initialization is needed
  ///
  this->initPool(catconnect);
  if( !this->initIOV(recordToTag) ){
    throw cond::Exception("NoIOVFound")<<"IOV not found for requested records";
  }
}


PoolDBESSource::~PoolDBESSource()
{
  this->closePool();
  delete m_session;
  delete m_loader;
}


//
// member functions
//
void 
PoolDBESSource::setIntervalFor( const edm::eventsetup::EventSetupRecordKey& iKey, const edm::IOVSyncValue& iTime, edm::ValidityInterval& oInterval ) {
  RecordToTypes::iterator itRec = m_recordToTypes.find( iKey.name() );
  //std::cout<<"setIntervalFor "<<iKey.name()<<std::endl;
  if( itRec == m_recordToTypes.end() ) {
    //no valid record
    //std::cout<<"no valid record "<<std::endl;
    oInterval = edm::ValidityInterval::invalidInterval();
    return;
  }
  RecordToIOV::iterator itIOV = m_recordToIOV.find( iKey.name() );
  if( itIOV == m_recordToIOV.end() ){
    //std::cout<<"no valid IOV found for record "<<iKey.name()<<std::endl;
    oInterval = edm::ValidityInterval::invalidInterval();
    return;
  }
  typedef std::map<unsigned long, std::string> IOVMap;
  typedef IOVMap::const_iterator iterator;
  pool::Ref<cond::IOV> myiov = itIOV->second;
  std::string payloadToken;
  unsigned long abtime=iTime.eventID().run();
  //valid time check
  //check if current run exceeds iov upperbound
  unsigned long myendoftime=myiov->iov.rbegin()->first;
  //std::cout<<"current time "<<abtime<<std::endl;
  //std::cout<<"myendoftime "<<myendoftime<<std::endl;
  if( myendoftime!=edm::IOVSyncValue::endOfTime().eventID().run() && abtime>myendoftime ){
    std::ostringstream os;
    os<<abtime;
    throw cond::noDataForRequiredTimeException("PoolDBESSource::setIntervalFor",iKey.name(),os.str());
  }
  iterator iEnd = myiov->iov.lower_bound( abtime );
  //std::cout<<"current time "<<abtime<<std::endl;
  if( iEnd == myiov->iov.end() ||  (*iEnd).second.empty() ) {
    //no valid data
    oInterval = edm::ValidityInterval(edm::IOVSyncValue::endOfTime(), edm::IOVSyncValue::endOfTime());
  } else {
    unsigned long starttime=edm::IOVSyncValue::beginOfTime().eventID().run();
    if (iEnd != myiov->iov.begin()) {
      iterator iStart(iEnd); iStart--;
      starttime = (*iStart).first+edm::IOVSyncValue::beginOfTime().eventID().run();
      //starttime = (*iStart).first;
    }
    payloadToken = (*iEnd).second;
    //std::cout<<"valid time "<<(*iEnd).first<<std::endl;
    //std::cout<<"payloadToken "<<payloadToken<<std::endl;
    //edm::IOVSyncValue start( edm::EventID(0,0) );
    edm::IOVSyncValue start( edm::EventID(starttime,0) );
    //std::cout<<"starttime "<<edm::EventID(starttime,0)<<std::endl;
    //std::cout<<"stop time "<<edm::EventID((*iEnd).first,0)<<std::endl;
    //edm::IOVSyncValue stop ( edm::EventID((*iEnd).first+edm::IOVSyncValue::beginOfTime().eventID().run(),0) );
    edm::IOVSyncValue stop ( edm::EventID((*iEnd).first).run(),0 );
    oInterval = edm::ValidityInterval( start, stop );
  }
  m_proxyToToken[buildName(itRec->first ,itRec->second)]=payloadToken;  
}   

void 
PoolDBESSource::registerProxies(const edm::eventsetup::EventSetupRecordKey& iRecordKey , KeyedProxies& aProxyList) 
{
  //std::cout<<"Entering PoolDBESSource::registerProxies"<<std::endl;
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
       //m_svc->transaction().start(pool::ITransaction::READ);
       boost::shared_ptr<DataProxy> proxy(cond::ProxyFactory::get()->create( buildName(iRecordKey.name(), type.name() ), &(m_session->DataSvc()), pos));
       //m_svc->transaction().commit();
       if(0 != proxy.get()) {
	 eventsetup::DataKey key( type, "");
	 aProxyList.push_back(KeyedProxies::value_type(key,proxy));
       }
     }else{
       //std::cout<<"IS default type "<<std::endl;
     }
     //cout <<endl;
   }
}


void 
PoolDBESSource::newInterval(const edm::eventsetup::EventSetupRecordKey& iRecordType,const edm::ValidityInterval& iInterval) 
{
  //std::cout<<"PoolDBESSource::newInterval "<<iRecordType.name()<<std::endl;
  invalidateProxies(iRecordType);
  //std::cout<<"invalidated "<<std::endl;
}

// ------------ method called to produce the data  ------------

//define this as a plug-in
DEFINE_FWK_EVENTSETUP_SOURCE(PoolDBESSource)
  
