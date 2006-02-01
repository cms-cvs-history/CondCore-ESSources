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
#include "CondFormats/Calibration/interface/Pedestals.h"
#include<iostream>
#include<string>
#include "POOLCore/POOLContext.h"
#include "SealKernel/Exception.h"
#include "SealBase/SharedLibrary.h"
#include "SealBase/SharedLibraryError.h"
#include <exception>
int main(int csize, char** cline ) {
  std::string connection1( "sqlite_file:ecalcalib.db");
  std::string connection2( "sqlite_file:calib.db");
  seal::PluginManager::get()->initialise();
  try {
    seal::SharedLibrary::load( seal::SharedLibrary::libname( "CondFormatsCalibration" ) );
    seal::SharedLibrary::load( seal::SharedLibrary::libname( "CondFormatsEcalObjects" ) );
  }catch ( seal::SharedLibraryError& error) 
    {
      std::cout<<"caught ShareLibraryError "<<error.explainSelf()<<std::endl;
    }
  try {
    // Loads the seal message stream
    pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
    std::cout<<1<<std::endl;
    pool::POOLContext::setMessageVerbosityLevel( seal::Msg::Debug );
    pool::POOLContext::loadComponent( "POOL/Services/EnvironmentAuthenticationService" );
    pool::URIParser p;
    p.parse();
    
    pool::IFileCatalog lcat1;
    pool::IFileCatalog * cat1 = &lcat1;
    cat1->setWriteCatalog(p.contactstring());
    cat1->connect();
    cat1->start();
    pool::IDataSvc *svc1 = pool::DataSvcFactory::instance(cat1);
    // Define the policy for the implicit file handling
    pool::DatabaseConnectionPolicy policy;
    policy.setWriteModeForNonExisting(pool::DatabaseConnectionPolicy::CREATE);
    // policy.setWriteModeForExisting(pool::DatabaseConnectionPolicy::OVERWRITE);
    policy.setWriteModeForExisting(pool::DatabaseConnectionPolicy::UPDATE);
    svc1->session().setDefaultConnectionPolicy(policy);
    std::cout<<"Reading "<<std::endl;
    std::string pedToken("[DB=5E7D42F1-0456-DA11-A339-0008743F7CC7][CNT=EcalPedestals][CLID=2F16F0A9-79D5-4881-CE0B-C271DD84A7F1][TECH=00000B00][OID=00000003-00000000]");
    svc1->transaction().start(pool::ITransaction::READ);
    pool::Ref<EcalPedestals> ped1(svc1,pedToken);
    std::cout<<"real pointer "<<&(*ped1)<<std::endl;
    //std::cout<<"channel 1858 "<<"mean_x1 " << (*ped).m_pedestals[1858].mean_x1 << ",rms_x1" << (*ped).m_pedestals[1858].rms_x1;
    std::cout << std::endl;


    
    pool::IFileCatalog lcat2;
    pool::IFileCatalog * cat2 = &lcat2;
    cat2->setWriteCatalog(p.contactstring());
    cat2->connect();
    cat2->start();
    pool::IDataSvc *svc2 = pool::DataSvcFactory::instance(cat1);
    // Define the policy for the implicit file handling
    //pool::DatabaseConnectionPolicy policy;
    //policy.setWriteModeForNonExisting(pool::DatabaseConnectionPolicy::CREATE);
    // policy.setWriteModeForExisting(pool::DatabaseConnectionPolicy::OVERWRITE);
    //policy.setWriteModeForExisting(pool::DatabaseConnectionPolicy::UPDATE);
    svc2->session().setDefaultConnectionPolicy(policy);
    std::cout<<"Reading "<<std::endl;
    std::string pedToken2("[DB=48BF9959-0556-DA11-9F86-0008743F7CC7][CNT=Pedestals][CLID=7E52FD73-8557-DA11-BEF0-0008743F7CC7][TECH=00000B00][OID=00000003-00000000]");
    svc2->transaction().start(pool::ITransaction::READ);
    pool::Ref<Pedestals> ped2(svc2,pedToken2);
    std::cout<<"real pointer "<<&(*ped2)<<std::endl;
    std::cout << std::endl;

    svc1->transaction().commit();
    svc1->session().disconnectAll();
    std::cout << "commit catalog1" << std::endl;
    cat1->commit();
    cat1->disconnect();
    delete svc1;

    svc2->transaction().commit();
    svc2->session().disconnectAll();
    std::cout << "commit catalog2" << std::endl;
    cat2->commit();
    cat2->disconnect();
    delete svc2;
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
