from __future__ import print_function, division

__author__ = 'saeedamen' # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.util.loggermanager import LoggerManager
from tcapy.conf.constants import Constants

class FXConv(object):
    """Various methods to manipulate FX crosses, applying correct conventions.

    """

    # TODO
    # g10 = ['EUR', 'GBP', 'AUD', 'NZD', 'USD', 'CAD', 'CHF', 'NOK', 'SEK', 'JPY']
    # order = ['XAU', 'XPT', 'XBT', 'XAG', 'EUR', 'GBP', 'AUD', 'NZD', 'USD', 'CAD', 'CHF', 'NOK', 'SEK', 'JPY']

    def __init__(self):
        self.logger = LoggerManager().getLogger(__name__)
        self.constants = Constants()
        return

    def g10_crosses(self):

        g10_crosses = []

        for i in range(0, len(self.constants.g10)):
            for j in range(0, len(self.constants.g10)):
                if i != j:
                    g10_crosses.append(self.correct_notation(self.constants.g10[i] + self.constants.g10[j]))

        set_val = set(g10_crosses)
        g10_crosses = sorted(list(set_val))

        return g10_crosses

    def em_or_g10(self, currency):

        try:
            index = self.constants.g10.index(currency)
        except ValueError:
            index = -1

        if (index < 0):
            return 'em'

        return 'g10'

    def is_USD_base(self, cross):
        base = cross[0:3]
        terms = cross[3:6]

        if base == 'USD':
            return True

        return False

    def is_EM_cross(self, cross):
        base = cross[0:3]
        terms = cross[3:6]

        if self.em_or_g10(base) == 'em' or self.em_or_g10(terms) == 'em':
            return True

        return False

    def reverse_notation(self, cross):
        base = cross[0:3]
        terms = cross[3:6]

        return terms + base

    def correct_notation(self, cross):

        if isinstance(cross, list):
            corrected_pairs = []

            for c in cross:
                corrected_pairs.append(self.correct_notation(c))

            return corrected_pairs

        base = cross[0:3]
        terms = cross[3:6]

        try:
            base_index = self.constants.quotation_order.index(base)
        except ValueError:
            base_index = -1

        try:
            terms_index = self.constants.quotation_order.index(terms)
        except ValueError:
            terms_index = -1

        if (base_index < 0 and terms_index > 0):
            return terms + base
        if (base_index > 0 and terms_index < 0):
            return base + terms
        elif (base_index > terms_index):
            return terms + base
        elif (terms_index > base_index):
            return base + terms

        return cross

    def currency_pair_in_list(self, currency_pair, list_of_pairs):
        return currency_pair in self.correct_notation(list_of_pairs)

    def correct_unique_notation_list(self, list_of_pairs):

        return list(set(self.correct_notation(list_of_pairs)))