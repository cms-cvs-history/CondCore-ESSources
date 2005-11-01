#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
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
  EcalPedestals* ped1=new EcalPedestals;
  int channelId;
  EcalPedestals::Item item;
  
  channelId=1656;
  item.mean_x1 =0.91;
  item.rms_x1  =0.17;
  item.mean_x6 =0.52;
  item.rms_x6  =0.03;
  item.mean_x12=0.16;
  item.rms_x12 =0.05;
  ped1->m_pedestals.insert(std::make_pair(channelId,item));
  
  channelId=1200;
  item.mean_x1=0.5;
  item.rms_x1=0.94;
  item.mean_x6 =0.72;
  item.rms_x6  =0.07;
  item.mean_x12=0.87;
  item.rms_x12 =0.07;
  ped1->m_pedestals.insert(std::make_pair(channelId,item));
  
  std::string ped1tok=w.write<EcalPedestals>(ped1, "EcalPedestals");//pool::Ref takes the ownership of ped1
  std::cout<<"ped1 token "<<ped1tok<<std::endl;
  //assign IOV
  unsigned long tillrun=5;
  pediov->iov.insert(std::make_pair(tillrun,ped1tok));
  
  EcalPedestals* ped2=new EcalPedestals; //the user gives up the object ownership upon send it to the writer
  channelId=1656;
  item.mean_x1=0.33;
  item.rms_x1=0.44;
  item.mean_x6 =0.22;
  item.rms_x6  =0.11;
  item.mean_x12=0.39;
  item.rms_x12 =0.12;
  ped2->m_pedestals.insert(std::make_pair(channelId,item));
  
  channelId=1200;
  item.mean_x1=0.56;
  item.rms_x1=0.98;
  item.mean_x6 =0.83;
  item.rms_x6  =0.27;
  item.mean_x12=0.54;
  item.rms_x12 =0.27;
  ped2->m_pedestals.insert(std::make_pair(channelId,item));
  
  std::string pedtok2=w.write<EcalPedestals>(ped2,"EcalPedestals");
  std::cout<<"ped2 token "<<pedtok2<<std::endl;
  
  //assign IOV
  tillrun=10;
  pediov->iov.insert(std::make_pair(tillrun,pedtok2));
  std::string pediovToken=w.write<cond::IOV>(pediov,"IOV");  
  std::cout<<"ped iov token "<<pediovToken<<std::endl;  
  w.commitTransaction();

  //register the iovToken to the metadata service
  cond::MetaData metadata_svc(contact);
  metadata_svc.addMapping("EcalPedestals_2008_test", pediovToken);  
}
