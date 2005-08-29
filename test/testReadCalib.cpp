#include "Reflection/Class.h"
#include "PluginManager/PluginManager.h"
#include "POOLCore/Token.h"
#include "FileCatalog/URIParser.h"
#include "FileCatalog/FCSystemTools.h"
#include "FileCatalog/IFileCatalog.h"
#include "StorageSvc/DbType.h"
#include "PersistencySvc/DatabaseConnectionPolicy.h"
#include "PersistencySvc/ISession.h"
#include "PersistencySvc/ITransaction.h"
#include "PersistencySvc/IDatabase.h"
#include "PersistencySvc/Placement.h"
#include "DataSvc/DataSvcFactory.h"
#include "DataSvc/IDataSvc.h"
#include "DataSvc/ICacheSvc.h"
#include "DataSvc/Ref.h"
#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include<iostream>
#include<string>
#include "POOLCore/POOLContext.h"
#include "SealKernel/Exception.h"

int main(int csize, char** cline ) {
  const std::string userNameEnv = "POOL_AUTH_USER=cms_xiezhen_dev";
  ::putenv( const_cast<char*>( userNameEnv.c_str() ) );
  const std::string passwdEnv = "POOL_AUTH_PASSWORD=xiezhen123";
  ::putenv( const_cast<char*>( passwdEnv.c_str() ) );
  std::string m_dbConnection( "oracle://devdb10/cms_xiezhen_dev");
  //std::string m_dbConnection( "sqlite_file:trackerped.db");
  std::cout<<"connecting..."<<m_dbConnection<<std::endl;
  seal::PluginManager::get()->initialise();
  try {
    // Loads the seal message stream
    pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
    pool::POOLContext::loadComponent( "POOL/Services/EnvironmentAuthenticationService" );
    pool::URIParser p;
    p.parse();
    
    pool::IFileCatalog lcat;
    pool::IFileCatalog * cat = &lcat;
    cat->setWriteCatalog(p.contactstring());
    cat->connect();
    cat->start();
    pool::IDataSvc *svc = pool::DataSvcFactory::instance(cat);
    // Define the policy for the implicit file handling
    pool::DatabaseConnectionPolicy policy;
    policy.setWriteModeForNonExisting(pool::DatabaseConnectionPolicy::CREATE);
    // policy.setWriteModeForExisting(pool::DatabaseConnectionPolicy::OVERWRITE);
    policy.setWriteModeForExisting(pool::DatabaseConnectionPolicy::UPDATE);
    svc->session().setDefaultConnectionPolicy(policy);
    std::cout<<"Reading "<<std::endl;
    std::string pedToken(cline[1]);
    svc->transaction().start(pool::ITransaction::READ);
    pool::Ref<EcalPedestals> ped(svc,pedToken);
    std::cout<<"real pointer "<<&(*ped)<<std::endl;
    std::cout<<"channel 1858 "<<"mean " << (*ped).m_pedestals[1858].m_mean << ",variance" << (*ped).m_pedestals[1858].m_variance;
    std::cout << std::endl;
    svc->transaction().commit();
    svc->session().disconnectAll();
    std::cout << "commit catalog" << std::endl;
    cat->commit();
    delete svc;
  }   
  catch ( seal::Exception& e ) {
    std::cout << e.what() << std::endl;
    return 1;
  }
  catch ( std::exception& e ) {
    std::cout << e.what() << std::endl;
    return 1;
  }
  catch ( ... ) {
    std::cout << "Funny error" << std::endl;
    return 1;
  }
  return 0;
}
