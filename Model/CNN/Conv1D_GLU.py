
import tf_keras as keras
from keras.layers import Layer, Conv1D, Multiply, Activation, Input, Dense, MaxPooling1D, Dropout, LSTM, Concatenate
from keras.models import Model
from keras import initializers
import tensorflow as tf

class Conv1DGLU(Layer):
    def __init__(self, filters, kernel_size, **kwargs):
        super(Conv1DGLU, self).__init__(**kwargs)
        self.filters = filters
        self.kernel_size = kernel_size
        self.conv_linear = Conv1D(filters, kernel_size,  padding='same')
        self.conv_gate = Conv1D(filters, kernel_size, padding='same')

    def call(self, inputs):
        linear_out = self.conv_linear(inputs)
        # gate_out = tf.keras.activations.sigmoid(self.conv_gate(inputs))
        gate_out = keras.activations.relu(self.conv_gate(inputs), max_value=6) / 6
        return Multiply()([linear_out, gate_out])
