
/*----------------------------------------------------------------------

Toy EDProducers and EDProducts for testing purposes only.

----------------------------------------------------------------------*/

#include <stdexcept>
#include <string>
#include <iostream>
#include <map>
#include "FWCore/Framework/interface/EDAnalyzer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/ESHandle.h"
#include "FWCore/Framework/interface/MakerMacros.h"

#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "CondFormats/DataRecord/interface/EcalPedestalsRcd.h"

using namespace std;

namespace edmtest
{
  class EcalPedestalsAnalyzer : public edm::EDAnalyzer
  {
  public:
    explicit  EcalPedestalsAnalyzer(edm::ParameterSet const& p) 
    { }
    explicit  EcalPedestalsAnalyzer(int i) 
    { }
    virtual ~ EcalPedestalsAnalyzer() { }
    virtual void analyze(const edm::Event& e, const edm::EventSetup& c);
  private:
  };
  
  void
   EcalPedestalsAnalyzer::analyze(const edm::Event& e, const edm::EventSetup& context)
  {
    using namespace edm::eventsetup;
    // Context is not used.
    std::cout <<" I AM IN RUN NUMBER "<<e.id().run() <<std::endl;
    std::cout <<" ---EVENT NUMBER "<<e.id().run() <<std::endl;
    edm::eventsetup::ESHandle<EcalPedestals> pPeds;
    context.get<EcalPedestalsRcd>().get(pPeds);
    //call tracker code
    //
    std::cout <<" Ecal peds for channel 1858 "<<std::endl;
    int channelID=1858;
    //EcalPedestals* myped=const_cast<EcalPedestals*>(pPeds.product());
    const EcalPedestals* myped=pPeds.product();
    std::map<int,EcalPedestals::Item>::const_iterator it=myped->m_pedestals.find(channelID);
    if( it!=myped->m_pedestals.end() ){
      std::cout << "mean " <<it->second.m_mean << ",variance" << it->second.m_variance << std::endl;    
    }
  }
  DEFINE_FWK_MODULE(EcalPedestalsAnalyzer)
}
