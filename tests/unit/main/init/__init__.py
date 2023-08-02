# This is an init for a dummy pacakge. It is used to test the pullModuleMembers function.

from snapred import pullModuleMembers

aII, localz = pullModuleMembers(__file__, __name__)
