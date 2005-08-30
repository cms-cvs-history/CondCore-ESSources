#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "CondFormats/EcalObjects/interface/EcalMapping.h"
#include "CondCore/IOVService/interface/IOV.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "FWCore/Framework/interface/IOVSyncValue.h"
#include "SealKernel/Service.h"
#include "POOLCore/POOLContext.h"
#include "SealKernel/Context.h"
#include <string>
#include <map>
#include <iostream>
int main(){
  //std::string contact("oracle://devdb10/cms_xiezhen_dev");
  std::string contact("sqlite_file:ecalcalib.db");
  pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
  pool::POOLContext::setMessageVerbosityLevel( seal::Msg::Error );
  const std::string userNameEnv = "POOL_AUTH_USER=cms_xiezhen_dev";
  ::putenv( const_cast<char*>( userNameEnv.c_str() ) );
  const std::string passwdEnv = "POOL_AUTH_PASSWORD=xiezhen123";
  ::putenv( const_cast<char*>( passwdEnv.c_str() ) );
  
  cond::DBWriter w(contact);
  w.startTransaction();
  cond::IOV* pediov=new cond::IOV;
  cond::IOV* mapiov=new cond::IOV;
  EcalPedestals* ped1=new EcalPedestals;
  int channelId;
  EcalPedestals::Item item;
 
  channelId=1878;
  item.m_mean=0.3;
  item.m_variance=0.4;
  ped1->m_pedestals.insert(std::make_pair(channelId,item));
  
  channelId=1858;
  item.m_mean=0.5;
  item.m_variance=0.94;
  ped1->m_pedestals.insert(std::make_pair(channelId,item));

  std::string ped1tok=w.write<EcalPedestals>(ped1, "EcalPedestals");//pool::Ref takes the ownership of ped1
  std::cout<<"ped1 token "<<ped1tok<<std::endl;
  //assign IOV
  int tillrun=5;
  pediov->iov.insert(std::make_pair(tillrun,ped1tok));
  
  EcalPedestals* ped2=new EcalPedestals; //the user gives up the object ownership upon send it to the writer
  channelId=1878;
  item.m_mean=0.33;
  item.m_variance=0.44;
  ped2->m_pedestals.insert(std::make_pair(channelId,item));
  
  channelId=1858;
  item.m_mean=0.56;
  item.m_variance=0.98;
  ped2->m_pedestals.insert(std::make_pair(channelId,item));
  
  std::string pedtok2=w.write<EcalPedestals>(ped2,"EcalPedestals");
  std::cout<<"ped2 token "<<pedtok2<<std::endl;
  
  std::string mapiovToken=w.write<cond::IOV>(mapiov,"IOV");
  std::cout<<"mapiovToken "<<mapiovToken<<std::endl;
  //assign IOV
  tillrun=10;
  pediov->iov.insert(std::make_pair(tillrun,pedtok2));
  std::string pediovToken=w.write<cond::IOV>(pediov,"IOV");  
  std::cout<<"ped iov token "<<pediovToken<<std::endl;

  EcalMapping* mymap=new EcalMapping;
  mymap->buildMapping();
  std::string mappingtok=w.write<EcalMapping>(mymap,"EcalMapping");
  
  mapiov->iov.insert(std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(), mappingtok));
  
  w.commitTransaction();
  //register the iovToken to the metadata service
  cond::MetaData metadata_svc(contact);
  metadata_svc.addMapping("EcalPedestals_2008_test", pediovToken);  
  metadata_svc.addMapping("EcalMapping_v1", mapiovToken);  
}
