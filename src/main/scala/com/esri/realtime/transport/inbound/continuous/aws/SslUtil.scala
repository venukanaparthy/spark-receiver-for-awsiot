package com.esri.realtime.transport.inbound.continuous.aws

import java.io.{ByteArrayInputStream, InputStream, InputStreamReader}
import java.nio.file.{Files, Paths}
import java.security.cert.{CertificateFactory,X509Certificate}
import java.security.{KeyPair, KeyStore, SecureRandom, Security}
import javax.net.ssl.{KeyManagerFactory, SSLContext, SSLSocketFactory, TrustManagerFactory}
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter
import org.bouncycastle.openssl.{PEMKeyPair, PEMParser}
import java.math.BigInteger

case class KeyStorePasswordPair(keyStore:KeyStore, keyPass:String)

/** Factory for KeyStorePasswordPair instances. */
object SslUtil {
  
  /**
   * Generate KeyStorePasswordPair from pem file paths.
   *  
   * @param certFilePath Certificate file path
   * @param keyFilePath Private key file path
   * @return KeyStorePasswordPair
   */
  def generateFromFilePath(certFilePath:String, keyFilePath:String):KeyStorePasswordPair = {
    Security.addProvider(new BouncyCastleProvider())
   
    // load Server certificate
    val certParser:PEMParser = new PEMParser(new InputStreamReader(new ByteArrayInputStream(Files.readAllBytes(Paths.get(certFilePath)))))
    val serverCertHolder:X509CertificateHolder = certParser.readObject.asInstanceOf[X509CertificateHolder]
    val serverCert:X509Certificate = convertToJavaCertificate(serverCertHolder)
    certParser.close()

    // load Private Key
    val keyParser:PEMParser = new PEMParser(new InputStreamReader(new ByteArrayInputStream(Files.readAllBytes(Paths.get(keyFilePath)))))
    val pemKeyPair:PEMKeyPair = keyParser.readObject.asInstanceOf[PEMKeyPair]
    val keyPair:KeyPair = new JcaPEMKeyConverter().getKeyPair(pemKeyPair)
    keyParser.close()
   
    // client key and certificates are sent to server so it can authenticate us
    val ks:KeyStore  = KeyStore.getInstance(KeyStore.getDefaultType())
    ks.load(null, null)
    ks.setCertificateEntry("alias", serverCert)
    val keyPassword =  new BigInteger(128, new SecureRandom()).toString(32)
    ks.setKeyEntry("alias", keyPair.getPrivate(), keyPassword.toCharArray, Array(serverCert))
 
    KeyStorePasswordPair(ks, keyPassword)       
  }


  def convertToJavaCertificate(certificateHolder:X509CertificateHolder):X509Certificate = {
     val is:InputStream = new ByteArrayInputStream(certificateHolder.toASN1Structure.getEncoded);
    try {
      CertificateFactory.getInstance("X.509").generateCertificate(is).asInstanceOf[X509Certificate]
    } finally is.close()
  }
}
