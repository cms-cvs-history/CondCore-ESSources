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

//#include "SealUtil/SealTimer.h"
//#include "SealUtil/TimingReport.h"
//#include "SealUtil/SealHRRTChrono.h"
//#include "SealUtil/SealHRChrono.h"

#include "CondFormats/Calibration/interface/Pedestals.h"
#include "CondCore/IOVService/interface/IOV.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "CLHEP/Random/RandFlat.h"
#include<iostream>
#include<string>

#include "POOLCore/POOLContext.h"
#include "SealKernel/Exception.h"


int main(int csize, char** cline ) {
  const std::string userNameEnv = "POOL_AUTH_USER=cms_xiezhen_dev";
  ::putenv( const_cast<char*>( userNameEnv.c_str() ) );
  const std::string passwdEnv = "POOL_AUTH_PASSWORD=xiezhen123";
  ::putenv( const_cast<char*>( passwdEnv.c_str() ) );
  //std::string m_dbConnection( "oracle://devdb10/cms_xiezhen_dev");
  std::string m_dbConnection( "sqlite_file:trackerped.db");
  std::cout<<"connecting..."<<m_dbConnection<<std::endl;
  seal::PluginManager::get()->initialise();
  //seal::TimingReport timRep;
  
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
    cond::MetaData meta(m_dbConnection);
    if (csize==1) { //write
      long int tech(pool::POOL_RDBMS_StorageType.type()); 
      svc->transaction().start(pool::ITransaction::UPDATE);
      pool::Ref<cond::IOV> pedIov(svc, new cond::IOV);
      pool::Placement iovPlace(m_dbConnection, pool::DatabaseSpecification::PFN, "IOV", pool::Guid::null(), tech); 
      pedIov.markWrite(iovPlace);
      std::string iovtoken=pedIov.toString();
      pool::Placement pedPlace(m_dbConnection, pool::DatabaseSpecification::PFN, "Pedestals", pool::Guid::null(), tech); 
      int totmodules=10;
      //int totmodules=16000;
      int iovn=0;
      int evtn=10;
      for (int j=0; j<evtn; ++j) {
	iovn=j%5;//till time
	if(iovn==0){
	  pool::Ref<Pedestals> ped(svc,new Pedestals);
	  ped.markWrite(pedPlace);
	  std::string pedtoken=ped.toString();
	  pedIov->iov[j+5]=pedtoken;
	  for(int l=0; l<totmodules; ++l){
	    for(int apv=0; apv<4; ++apv){
	      for(int channel=0; channel<128; ++channel){
		Pedestals::Item item;
		item.m_mean=RandFlat::shoot(0.0,0.1);
		item.m_variance=RandFlat::shoot(-0.01,0.01);
		ped->m_pedestals.push_back(item);
	      }
	    }
	  }
	}
      }
      svc->transaction().commit();
      std::cout << "iov size " << pedIov->iov.size() << std::endl;
      svc->session().disconnectAll();
      std::cout << "committed" << std::endl;
      std::cout << "iov tokens "<<iovtoken<< std::endl;
      if( meta.addMapping("TrackerCalibration",iovtoken) ){
	std::cout<<"new mapping inserted "<<std::endl;
      }else{
	std::cout<<"mapping exists, do nothing "<<std::endl;
      }
    }else {
      std::cout<<"Reading "<<std::endl;
      std::string iovAName(cline[1]);
      std::string iovAToken= meta.getToken(iovAName);
      svc->transaction().start(pool::ITransaction::READ);
      pool::Ref<cond::IOV> iovA(svc,iovAToken);
      std::pair<unsigned long,std::string> iovpair=*iovA->iov.lower_bound(7);
      std::cout<<"iov idx "<<iovpair.first<<std::endl;
      std::string pedCid =iovpair.second;
      std::cout << "ped Cid " << pedCid << std::endl;
      pool::Ref<Pedestals> ped(svc,pedCid);
      std::cout << "ped for 100" <<std::endl;
      std::cout << "mean " << (*ped).m_pedestals[100].m_mean << ",variance" 
		<< (*ped).m_pedestals[100].m_variance;
      std::cout << std::endl;
      
    }//end else
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
  
  // timRep.dump();
  
  std::cout << "finished" << std::endl;
  
  return 0;
}















