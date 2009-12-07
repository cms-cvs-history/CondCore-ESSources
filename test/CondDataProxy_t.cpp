// test DataProxy
#include "FWCore/PluginManager/interface/PluginManager.h"
#include "FWCore/PluginManager/interface/standard.h"
#include "FWCore/ServiceRegistry/interface/ServiceRegistry.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

#include "CondCore/Utilities/interface/CondPyInterface.h"


#include "CondCore/DBCommon/interface/Exception.h"

#include "CondCore/DBCommon/interface/DbConnection.h"
#include "CondCore/DBCommon/interface/DbTransaction.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/FipProtocolParser.h"
#include "CondCore/MetaDataService/interface/MetaData.h"


#include "CondCore/DBCommon/interface/Time.h"
#include "CondCore/Utilities/interface/CommonOptions.h"


#include "CondCore/IOVService/interface/IOVProxy.h"
#include "CondFormats/Common/interface/PayloadWrapper.h"
#include "CondCore/DBCommon/interface/TypedRef.h"

#include "CondCore/PluginSystem/interface/DataProxy.h"
#include "CondCore/PluginSystem/interface/ProxyFactory.h"
#include "CondCore/IOVService/interface/PayloadProxy.h"

#include "CondCore/Utilities/interface/CommonOptions.h"

#include "CondCore/DBCommon/interface/ClassID.h"

#include "CondCore/IOVService/interface/KeyList.h"
#include "CondCore/IOVService/interface/KeyListProxy.h"


#include <boost/program_options.hpp>
#include <iterator>
#include <iostream>

#include <typeinfo>


// emulate ESSource

namespace {
  std::string
  buildName( const std::string& iRecordName) {
    return iRecordName+"@NewProxy";
  }



  class CondGetterFromTag : public cond::CondGetter {
  public:
    CondGetterFromTag( cond::CondDB  db, std::string tag ) : 
      m_db(db), m_tag(tag){}
    virtual ~CondGetterFromTag(){}

    cond::IOVProxy get(std::string name) const {
      // we do not use the name: still verify that is correct...
      std::cout << "keyed record name " << name << std::endl;
      return m_db.iov(m_tag);
    }

    cond::CondDB  m_db;
    std::string m_tag;
  };

  // make compiler happy
  // namespace { const char * sourceRecordName_ = 0;} 


}

int main( int argc, char** argv ){
  edmplugin::PluginManager::configure(edmplugin::standard::config());
  cond::CommonOptions myopt("CondDataProxy_t");
  myopt.addConnect();
  myopt.addAuthentication(true);
  myopt.visibles().add_options()
    ("verbose,v","verbose")
    ("tag,t",boost::program_options::value<std::string>(),"tag")
    ("keyed,k",boost::program_options::value<std::string>(),"tag of keyed container")
    ("record,r",boost::program_options::value<std::string>(),"record")
    ("atTime,a",boost::program_options::value<cond::Time_t>(),"time of event")
    ;

  myopt.description().add( myopt.visibles() );
  boost::program_options::variables_map vm;
  try{
    boost::program_options::store(boost::program_options::command_line_parser(argc, argv).options(myopt.description()).run(), vm);
    boost::program_options::notify(vm);
  }catch(const boost::program_options::error& er) {
    std::cerr << er.what()<<std::endl;
    return 1;
  }
  if (vm.count("help")) {
    std::cout << myopt.visibles() <<std::endl;;
    return 0;
  }


  //  bool verbose=vm.count("verbose");
  bool debug=vm.count("debug");
  
  std::string connect;
  std::string authPath("");
  std::string user("");
  std::string pass("");

  std::string tag;
  std::string keyed;
  std::string record;
  cond::Time_t time=0;

  if(!vm.count("connect")){
    std::cerr <<"[Error] no connect[c] option given \n";
    std::cerr<<" please do "<<argv[0]<<" --help \n";
    return 1;
  }else{
    connect=vm["connect"].as<std::string>();
  }
  if(vm.count("user")){
    user=vm["user"].as<std::string>();
  }
  if(vm.count("pass")){
    pass=vm["pass"].as<std::string>();
  }
  if( vm.count("authPath") ){
      authPath=vm["authPath"].as<std::string>();
  }

  if(vm.count("keyed")){
    keyed=vm["keyed"].as<std::string>();
  }
  if(vm.count("tag")){
    tag=vm["tag"].as<std::string>();
  }
  if(vm.count("record")){
    record=vm["record"].as<std::string>();
  }
  if(vm.count("atTime")){
    time=vm["atTime"].as<cond::Time_t>();
  }


  std::vector<edm::ParameterSet> psets;
  
  edm::ParameterSet pSet;
  pSet.addParameter("@service_type",std::string("SiteLocalConfigService"));
  psets.push_back(pSet);

  edm::ServiceToken services(edm::ServiceRegistry::createSet(psets));
  edm::ServiceRegistry::Operate operate(services);

  try{


  cond::RDBMS rdbms(authPath, debug);
  cond::CondDB db = rdbms.getDB(connect);
  cond::DbSession mysession = db.session();


  // here the proxy is constructed: 
  cond::DataProxyWrapperBase * pb =  
    cond::ProxyFactory::get()->create(buildName(record),  mysession, 
				      cond::DataProxyWrapperBase::Args(db.iovToken(tag), ""));

  cond::DataProxyWrapperBase::ProxyP  payloadProxy = pb->proxy();

  std::cout << cond::className(typeid(*payloadProxy)) << std::endl;

  CondGetterFromTag getter(db,keyed);
  payloadProxy->loadMore(getter);

  cond::ValidityInterval iov = payloadProxy->setIntervalFor(time);
  payloadProxy->make();
  std::cout << "for " << time
	    <<": since "<< iov.first
	     <<", till "<< iov.second;
  if (payloadProxy->isValid()) {
  } else
    std::cout << ". No data";
  std::cout << std::endl;
  

  {
    
    cond::PayloadProxy<cond::KeyList> const * pp = 
      dynamic_cast< cond::PayloadProxy<cond::KeyList>* >(payloadProxy.get());
    if (pp) {
      const cond::KeyList & keys = (*pp)();
      int n=0;
      for (int i=0; i<keys.size(); i++)
	if (keys.elem(i)) n++; 
      std::cout << "found " << n << " valid keyed confs" << std::endl;
      
    }
  }
    

  }catch(const cond::Exception& er){
    std::cout<<"error "<<er.what()<<std::endl;
  }catch(const std::exception& er){
    std::cout<<"std error "<<er.what()<<std::endl;
  }
  return 0;
}
