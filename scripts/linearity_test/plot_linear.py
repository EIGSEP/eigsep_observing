import numpy as np                                                                    
import matplotlib.pyplot as plt                           

d = np.load("linearity.npz")
plt.plot(d["attenuation_dB"], d["rms_x"], "o-", label="Pol X (N0)")
plt.plot(d["attenuation_dB"], d["rms_y"], "s-", label="Pol Y (E2)")
plt.yscale("log")
plt.xlabel("Attenuation (dB)")                                                        
plt.ylabel("RMS (ADC counts)")                                                        
plt.legend()
plt.grid(True)                                                                        
plt.tight_layout()                                        
plt.show()
