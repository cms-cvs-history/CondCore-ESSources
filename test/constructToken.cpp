//#include "Reflection/Class.h"
#include "PluginManager/PluginManager.h"
#include "POOLCore/Token.h"
#include "FileCatalog/URIParser.h"
#include "FileCatalog/FCSystemTools.h"
#include "FileCatalog/IFileCatalog.h"
#include "StorageSvc/DbType.h"
#include "PersistencySvc/DatabaseConnectionPolicy.h"
#include "PersistencySvc/ISession.h"
#include "PersistencySvc/ITransaction.h"
//#include "PersistencySvc/IDatabase.h"
//#include "PersistencySvc/Placement.h"
#include "DataSvc/DataSvcFactory.h"
#include "DataSvc/IDataSvc.h"
//#include "DataSvc/ICacheSvc.h"
#include "DataSvc/Ref.h"

#include "RelationalAccess/RelationalException.h"
#include "RelationalAccess/IRelationalService.h"
#include "RelationalAccess/IRelationalDomain.h"
#include "RelationalAccess/IRelationalSession.h"
#include "RelationalAccess/IRelationalTransaction.h"
#include "RelationalAccess/IRelationalSchema.h"
#include "RelationalAccess/IRelationalTable.h"
#include "RelationalAccess/IRelationalTableDataEditor.h"
#include "RelationalAccess/IRelationalQuery.h"
#include "RelationalAccess/IRelationalCursor.h"
#include "AttributeList/AttributeList.h"
#include "POOLCore/POOLContext.h"
#include "SealKernel/Context.h"
#include "StorageSvc/DbType.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
//#include "CondCore/IOVService/interface/IOV.h"
//#include "CondCore/MetaDataService/interface/MetaData.h"
#include<iostream>
#include<string>
#include <stdexcept>
#include <memory>
#include "SealKernel/Exception.h"

int main(int csize, char** cline ) {
  const std::string userNameEnv = "POOL_AUTH_USER=cms_xiezhen_dev";
  ::putenv( const_cast<char*>( userNameEnv.c_str() ) );
  const std::string passwdEnv = "POOL_AUTH_PASSWORD=xiezhen123";
  ::putenv( const_cast<char*>( passwdEnv.c_str() ) );
  std::string dbConnection( "oracle://devdb10/cms_xiezhen_dev");
  //std::string m_dbConnection( "sqlite_file:trackerped.db");
  std::cout<<"connecting..."<<dbConnection<<std::endl;
  seal::PluginManager::get()->initialise();
  try {
    // Loads the seal message stream
    pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
    pool::POOLContext::setMessageVerbosityLevel( seal::Msg::Error );
    pool::POOLContext::loadComponent( "POOL/Services/EnvironmentAuthenticationService" );
    pool::POOLContext::loadComponent( "POOL/Services/RelationalService" );
    seal::IHandle<pool::IRelationalService> serviceHandle = pool::POOLContext::context()->query<pool::IRelationalService>( "POOL/Services/RelationalService" );
    if ( ! serviceHandle ) {
      throw std::runtime_error( "Could not retrieve the relational service" );
    }
    pool::IRelationalDomain& domain = serviceHandle->domainForConnection( dbConnection );
    // Creating a session
    std::auto_ptr< pool::IRelationalSession > session( domain.newSession( dbConnection ) );
    // Establish a connection with the server
    std::cout << "Connecting..." << std::endl;
    if ( ! session->connect() ) {
      throw std::runtime_error( "Could not connect to the database server." );
    }
    // Start a transaction
    std::cout << "Starting a new transaction" << std::endl;
    if ( ! session->transaction().start() ) {
      throw std::runtime_error( "Could not start a new transaction." );
    }
    pool::IRelationalTable& table=session->userSchema().tableHandle("PEDESTALS");
    std::cout<< "Querying : SELECT IOV_VALUE_ID, TILLTIME FROM PEDESTALS"<< std::endl;
    std::auto_ptr< pool::IRelationalQuery > query1( table.createQuery() );
    query1->setRowCacheSize( 10 );
    query1->addToOutputList( "IOV_VALUE_ID" );
    query1->addToOutputList( "TILLTIME" );
    pool::IRelationalCursor& cursor1 = query1->process();
    std::map<long, std::string> iovmap;
    pool::Token tk;
    tk.setDb("3E60FA40-D105-DA11-981C-000E0C4DE431");//PFN GUID
    tk.setCont("Pedestals");
    pool::Guid classid(std::string("2F16F0A9-79D5-4881-CE0B-C271DD84A7F1"));//CLASSID GUID
    tk.setClassID(classid);
    tk.setTechnology(pool::POOL_RDBMS_StorageType.type());
    tk.oid().first=0;
    if( cursor1.start() ){
      while( cursor1.next() ) {
	const pool::AttributeList& row = cursor1.currentRow();
	long myl;
	row["IOV_VALUE_ID"].getValue<long>(myl);
	tk.oid().second=int(myl);
	std::string mytoken=tk.toString();
	std::cout<<"made token "<< mytoken<<std::endl;
	long mytime;
	row["TILLTIME"].getValue<long>(mytime);
	iovmap.insert(std::make_pair(mytime,mytoken));
	std::cout<<"IOV_VALUE_ID "<<myl<<std::endl;
	std::cout<<"TOKEN "<<mytoken<<std::endl;
	std::cout<<"TILLTIME "<<mytime<<std::endl;
      }
    } 
    pool::URIParser p;
    p.parse();   
    pool::IFileCatalog lcat;
    pool::IFileCatalog * cat = &lcat;
    cat->setWriteCatalog(p.contactstring());
    cat->connect();
    cat->start();
    pool::IDataSvc *svc = pool::DataSvcFactory::instance(cat);
    pool::DatabaseConnectionPolicy policy;
    policy.setWriteModeForNonExisting(pool::DatabaseConnectionPolicy::CREATE);
    policy.setWriteModeForExisting(pool::DatabaseConnectionPolicy::UPDATE);
    svc->session().setDefaultConnectionPolicy(policy);
    svc->transaction().start(pool::ITransaction::READ);
    std::map<long, std::string>::const_iterator it=iovmap.lower_bound(1997);
    if(it == iovmap.end() ){
      std::cout<<"no valid interval found "<<std::endl;
      exit(-1);
    }
    std::string mytoken=(*it).second;
    std::cout<<"reading Ped back by token "<<mytoken<<std::endl;
    pool::Ref<Pedestals> ped(svc,mytoken);
    std::cout << "mean " << (*ped).m_pedestals[0].m_mean << ",variance" 
	      << (*ped).m_pedestals[0].m_variance<<std::endl;
    std::cout << "mean " << (*ped).m_pedestals[1].m_mean << ",variance" 
	      << (*ped).m_pedestals[1].m_variance<<std::endl;
    std::cout<<std::endl;
    cat->commit();
    svc->transaction().commit();
    svc->session().disconnectAll();
    delete svc;
    
  }catch ( seal::Exception& e ) {
    std::cout << e.what() << std::endl;
    return 1;
  }catch ( std::exception& e ) {
    std::cout << e.what() << std::endl;
    return 1;
  }catch ( ... ) {
    std::cout << "Funny error" << std::endl;
    return 1;
  }
  std::cout << "finished" << std::endl;
  
  return 0;
}















