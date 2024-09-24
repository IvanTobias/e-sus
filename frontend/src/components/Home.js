import React from 'react';

const Home = () => {
  return (
    <div className="home-container">
      <header className="home-header">
        <h1>Bem-vindo ao Projeto!</h1>
        <p>Agradecemos por contribuir para o crescimento da nossa comunidade!</p>
      </header>
    <form>

      <main className="home-main">
        <section className="qr-section">
          <h2>Contribua com o Projeto</h2>
          <p>Escaneie o código QR abaixo ou use a chave Pix para contribuir:</p>
          
          {/* Exibindo o QR Code */}
          <div className="qr-code-container">
            <img 
              src="/qrcode.jpg" 
              alt="QR Code para pagamento via Pix" 
              className="qr-code" 
            />
          </div>

          {/* Exibindo a chave Pix */}
          <div className="pix-details">
            <p><strong>Chave Pix:</strong> 6ac7cf02-5202-46f5-ab89-aae064c178f3</p>
            {/*<p><strong>Nome:</strong> Ivan Tobias de Paula</p>*/}
            <p><strong>Valor:</strong> R$ 19,90</p>
          </div>
        </section>

        <section className="thank-you-section">
          <h3>Obrigado por fazer parte da comunidade!!</h3>
          <p>Sua contribuição nos ajuda a continuar desenvolvendo este projeto!</p>
        </section>
      </main>

      <footer className="home-footer">
      </footer>
      </form>

    </div>
  );
};

export default Home;
